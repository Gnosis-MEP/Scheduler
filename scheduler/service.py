import functools
import threading

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.tracer import BaseTracerService
from event_service_utils.tracing.jaeger import init_tracer


class Scheduler(BaseTracerService):
    def __init__(self,
                 service_stream_key, service_cmd_key,
                 event_dispatcher_data_key,
                 stream_factory,
                 logging_level,
                 tracer_configs):
        tracer = init_tracer(self.__class__.__name__, **tracer_configs)
        super(Scheduler, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key=service_cmd_key,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer,
        )
        self.cmd_validation_fields = ['id', 'action']
        self.data_validation_fields = ['id', 'buffer_stream_key']
        self.event_dispatcher_data = self.stream_factory.create(key=event_dispatcher_data_key, stype='streamOnly')

        self.bufferstream_to_dataflow = {
            'f32c1d9e6352644a5894305ecb478b0d': [['object-detection-data'], ['wm-data']]
        }

    def execute_adaptive_plan(self, dataflow):
        self.bufferstream_to_dataflow = dataflow

    @functools.lru_cache(maxsize=5)
    def get_destination_streams(self, destination):
        return self.stream_factory.create(destination, stype='streamOnly')

    def send_event_to_first_service_in_dataflow(self, event_data):

        next_destinations = event_data['data_flow'][0]
        for destination in next_destinations:
            self.logger.debug(f'Sending event to "{destination}": {event_data}')
            destination_stream = self.get_destination_streams(destination)
            self.write_event_with_trace(event_data, destination_stream)

    def apply_dataflow_to_event(self, event_data):
        buffer_stream_key = event_data['buffer_stream_key']
        data_flow = self.bufferstream_to_dataflow.get(buffer_stream_key, None)
        if data_flow is not None:
            event_data.update({
                'data_flow': data_flow,
                'data_path': [],
            })
        return event_data

    @timer_logger
    def process_data_event(self, event_data, json_msg):
        if not super(Scheduler, self).process_data_event(event_data, json_msg):
            return False
        new_event_data = self.apply_dataflow_to_event(event_data)
        self.send_event_to_first_service_in_dataflow(new_event_data)

    def process_action(self, action, event_data, json_msg):
        if not super(Scheduler, self).process_action(action, event_data, json_msg):
            return False
        if action == 'executeAdaptivePlan':
            dataflow = event_data['dataflow']
            self.execute_adaptive_plan(dataflow)

    def log_state(self):
        super(Scheduler, self).log_state()
        self._log_dict('Bufferstream to Dataflow', self.bufferstream_to_dataflow)

    def run(self):
        super(Scheduler, self).run()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.data_thread = threading.Thread(target=self.run_forever, args=(self.process_data,))
        self.cmd_thread.start()
        self.data_thread.start()
        self.cmd_thread.join()
        self.data_thread.join()
