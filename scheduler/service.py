import functools
import random
import threading

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.tracer import BaseTracerService
from event_service_utils.tracing.jaeger import init_tracer

from .strategies.weighted_rand import WeightedRandomStrategy
from .strategies.single_best_dataflow import SingleBestStrategy
from .strategies.round_robin import RoundRobinStrategy
from .strategies.random import RandomStrategy


class Scheduler(BaseTracerService):
    def __init__(self,
                 service_stream_key, service_cmd_key,
                 event_dispatcher_data_key,
                 scheduling_strategy,
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
            # 'f32c1d9e6352644a5894305ecb478b0d': [['object-detection-data'], ['wm-data']]
        }
        self.setup_scheduling_strategies()

    def setup_scheduling_strategies(self):
        self.scheduling_strategies = {
            'weighted_random': WeightedRandomStrategy(self),
            'random': RandomStrategy(self),
            'single_best': SingleBestStrategy(self),
            'round_robin': RoundRobinStrategy(self),
        }
        default_strategy = 'weighted_random'
        self.current_strategy = self.scheduling_strategies[default_strategy]

    def execute_adaptive_plan(self, strategy_data):
        strategy_name = strategy_data['name']
        strategy = self.scheduling_strategies.get(strategy_name)
        if strategy is None:
            self.logger.error(f'No strategy named "{strategy_name}" available that meets adaptive plan.')
            self.logger.error(f'Will ignore new stragegy plan.')
            return
        self.current_strategy = strategy
        self.current_strategy.update(strategy_data)

    @functools.lru_cache(maxsize=5)
    def get_destination_streams(self, destination):
        return self.stream_factory.create(destination, stype='streamOnly')

    def send_event_to_first_service_in_dataflow(self, event_data):
        event_dataflow = event_data.get('data_flow', [[]])
        next_destinations = event_dataflow[0] if len(event_dataflow) != 0 else []
        for destination in next_destinations:
            self.logger.debug(f'Sending event to "{destination}": {event_data}')
            destination_stream = self.get_destination_streams(destination)
            self.write_event_with_trace(event_data, destination_stream)

    def get_random_buffer_stream_dataflow(self):
        return random.choice(self._random_bufferstream_to_dataflow)

    def get_bufferstream_dataflow(self, buffer_stream_key):
        return self.current_strategy.get_bufferstream_dataflow(buffer_stream_key)
        # if 'random' in self.scheduling_strategy:
        #     data_flow = self.get_random_buffer_stream_dataflow()
        # elif self.scheduling_strategy == 'self-adaptive':
        #     data_flow = self.bufferstream_to_dataflow.get(buffer_stream_key, [])
        # return data_flow

    def apply_dataflow_to_event(self, event_data):
        buffer_stream_key = event_data['buffer_stream_key']
        data_flow = self.get_bufferstream_dataflow(buffer_stream_key)
        if len(data_flow) != 0:
            event_data.update({
                'data_flow': data_flow,
                'data_path': [],
            })
        else:
            self.logger.warning(f'Event data wihout a known buffer stream dataflow plan: {event_data}. Ignoring event.')
            return None
        return event_data

    @timer_logger
    def process_data_event(self, event_data, json_msg):
        if not super(Scheduler, self).process_data_event(event_data, json_msg):
            return False
        new_event_data = self.apply_dataflow_to_event(event_data)
        if new_event_data:
            self.send_event_to_first_service_in_dataflow(new_event_data)

    def process_action(self, action, event_data, json_msg):
        if not super(Scheduler, self).process_action(action, event_data, json_msg):
            return False
        if action == 'executeAdaptivePlan':
            if 'strategy' not in event_data:
                self.logger.info('Backward compat, not strategy, enforcing single best')
                scheduling_strategy = {
                    'name': 'single_best',
                    'dataflows': {
                        k: [[None, v]]
                        for k, v in event_data['dataflow'].items()
                    }
                }
            else:
                scheduling_strategy = event_data['strategy']
            self.execute_adaptive_plan(scheduling_strategy)

    def log_state(self):
        super(Scheduler, self).log_state()
        self._log_dict('Bufferstream to Dataflow', self.bufferstream_to_dataflow)
        self.current_strategy.log_state()
        # self.logger.info(f'Scheduling Stratefy: {self.scheduling_strategy}')
        # if 'random' in self.scheduling_strategy:
        #     self.logger.info(f'Random dataflows: {self._random_bufferstream_to_dataflow}')

    def run(self):
        super(Scheduler, self).run()
        self.log_state()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.data_thread = threading.Thread(target=self.run_forever, args=(self.process_data,))
        self.cmd_thread.start()
        self.data_thread.start()
        self.cmd_thread.join()
        self.data_thread.join()
