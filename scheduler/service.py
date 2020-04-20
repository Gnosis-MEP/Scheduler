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

        self.bufferstream_to_dataflow = {}

    def execute_adaptive_plan(self, dataflow):
        self.bufferstream_to_dataflow = dataflow

    def send_event_to_dispatcher(self, event_data):
        self.logger.debug(f'Sending event to Event Dispather: {event_data}')
        self.write_event_with_trace(event_data, self.event_dispatcher_data)

    def apply_dataflow_to_event(self, event_data):
        buffer_stream_key = event_data['buffer_stream_key']
        data_flow = self.bufferstream_to_dataflow.get(buffer_stream_key, None)
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
        self.send_event_to_dispatcher(new_event_data)

    def process_data(self):
        stream_sources_events = list(self.all_events_consumer_group.read_stream_events_list(count=1))
        if stream_sources_events:
            self.logger.debug(f'Processing DATA.. {stream_sources_events}')

        for stream_key_bytes, event_list in stream_sources_events:
            stream_key = stream_key_bytes
            if type(stream_key_bytes) == bytes:
                stream_key = stream_key_bytes.decode('utf-8')
            for event_tuple in event_list:
                event_id, json_msg = event_tuple
                try:
                    event_data = self.default_event_deserializer(json_msg)
                    event_data.update({
                        'buffer_stream_key': stream_key
                    })
                    self.process_data_event_wrapper(event_data, json_msg)
                except Exception as e:
                    self.logger.error(f'Error processing {json_msg}:')
                    self.logger.exception(e)

    def process_action(self, action, event_data, json_msg):
        if not super(Scheduler, self).process_action(action, event_data, json_msg):
            return False
        if action == 'executeAdaptivePlan':
            dataflow = event_data['dataflow']
            self.execute_adaptive_plan(dataflow)
        elif action == 'otherAction':
            # do some other action
            pass

    def log_state(self):
        super(Scheduler, self).log_state()
        self.logger.info(f'My service name is: {self.name}')

    def run(self):
        super(Scheduler, self).run()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.data_thread = threading.Thread(target=self.run_forever, args=(self.process_data,))
        self.cmd_thread.start()
        self.data_thread.start()
        self.cmd_thread.join()
        self.data_thread.join()
