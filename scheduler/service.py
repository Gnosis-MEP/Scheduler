import functools
import random
import threading

from event_service_utils.logging.decorators import timer_logger
from event_service_utils.services.event_driven import BaseEventDrivenCMDService, tags, EVENT_ID_TAG
from event_service_utils.tracing.jaeger import init_tracer

from .strategies.weighted_rand import WeightedRandomStrategy
from .strategies.single_best_dataflow import SingleBestStrategy
from .strategies.round_robin import RoundRobinStrategy
from .strategies.random import RandomStrategy

from .conf import (
    LISTEN_EVENT_TYPE_NEW_QUERY_SCHEDULING_PLANNED,
    LISTEN_EVENT_TYPE_SERVICE_WORKER_SLR_PROFILE_PLANNED,
    LISTEN_EVENT_TYPE_SERVICE_WORKER_OVERLOADED_PLANNED,
    LISTEN_EVENT_TYPE_SERVICE_WORKER_BEST_IDLE_PLANNED,
    LISTEN_EVENT_TYPE_UNNECESSARY_LOAD_SHEDDING_PLANNED,
    PUB_EVENT_TYPE_SCHEDULING_PLAN_EXECUTED,
)


class Scheduler(BaseEventDrivenCMDService):
    def __init__(self,
                 service_stream_key, service_cmd_key_list,
                 pub_event_list, service_details,
                 stream_factory,
                 default_scheduling_strategy,
                 logging_level,
                 tracer_configs):
        tracer = init_tracer(self.__class__.__name__, **tracer_configs)
        super(Scheduler, self).__init__(
            name=self.__class__.__name__,
            service_stream_key=service_stream_key,
            service_cmd_key_list=service_cmd_key_list,
            pub_event_list=pub_event_list,
            service_details=service_details,
            stream_factory=stream_factory,
            logging_level=logging_level,
            tracer=tracer,
        )
        self.cmd_validation_fields = ['id']
        self.data_validation_fields = ['id', 'buffer_stream_key']

        self.bufferstream_to_dataflow = {
            # 'f32c1d9e6352644a5894305ecb478b0d': [['object-detection-data'], ['wm-data']]
        }
        self.setup_scheduling_strategies(default_scheduling_strategy)

    def setup_scheduling_strategies(self, default_scheduling_strategy):
        self.scheduling_strategies = {
            'QQoS-W-HP': WeightedRandomStrategy(self),
            'QQoS-W-HP-LS': WeightedRandomStrategy(self),
            'random': RandomStrategy(self),
            'QQoS-TK-LP': SingleBestStrategy(self),
            'QQoS-TK-LP-LS': SingleBestStrategy(self),
            'round_robin': RoundRobinStrategy(self),
        }
        default_scheduling_strategy = 'QQoS-W-HP'
        self.current_strategy = self.scheduling_strategies[default_scheduling_strategy]

    def publish_scheduling_plan_executed(self, adaptive_plan):
        event_type = PUB_EVENT_TYPE_SCHEDULING_PLAN_EXECUTED
        new_event_data = {
            'id': self.service_based_random_event_id(),
            'plan': adaptive_plan
        }
        self.publish_event_type_to_stream(event_type=event_type, new_event_data=new_event_data)

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

    def apply_dataflow_to_event(self, event_data):
        buffer_stream_key = event_data['buffer_stream_key']
        data_flow = self.get_bufferstream_dataflow(buffer_stream_key)

        # if is load shedding
        if data_flow is None:
            self.event_trace_for_method_with_event_data(
                method=self.log_event_load_shedding,
                method_args=(),
                method_kwargs={
                    'event_data': event_data,
                },
                get_event_tracer=True,
                tracer_tags={
                    tags.SPAN_KIND: tags.SPAN_KIND_CONSUMER,
                    'bufferstream': buffer_stream_key,
                    EVENT_ID_TAG: event_data['id'],
                }
            )
            return None

        # if is load shedding because there is no proper plan to handle this event
        if len(data_flow) != 0:
            event_data.update({
                'data_flow': data_flow,
                'data_path': [],
            })
        else:
            self.logger.warning(f'Event data wihout a known buffer stream dataflow plan: {event_data}. Ignoring event.')
            return None
        return event_data

    def log_event_load_shedding(self, event_data):
        self.logger.debug(f'[Load shedding] dropping event: {event_data}')

    @timer_logger
    def process_data_event(self, event_data, json_msg):
        if not super(Scheduler, self).process_data_event(event_data, json_msg):
            return False
        new_event_data = self.apply_dataflow_to_event(event_data)
        if new_event_data:
            self.send_event_to_first_service_in_dataflow(new_event_data)

    def process_adaptive_plan(self, event_data):
        adaptive_plan = event_data['plan']
        execution_plan = adaptive_plan['execution_plan']
        scheduling_strategy = execution_plan['strategy']
        self.execute_adaptive_plan(scheduling_strategy)
        self.publish_scheduling_plan_executed(event_data['plan'])

    def process_event_type(self, event_type, event_data, json_msg):
        if not super(Scheduler, self).process_event_type(event_type, event_data, json_msg):
            return False
        plan_requests_types = [
            LISTEN_EVENT_TYPE_NEW_QUERY_SCHEDULING_PLANNED,
            LISTEN_EVENT_TYPE_SERVICE_WORKER_SLR_PROFILE_PLANNED,
            LISTEN_EVENT_TYPE_SERVICE_WORKER_OVERLOADED_PLANNED,
            LISTEN_EVENT_TYPE_SERVICE_WORKER_BEST_IDLE_PLANNED,
            LISTEN_EVENT_TYPE_UNNECESSARY_LOAD_SHEDDING_PLANNED,
        ]
        if event_type in plan_requests_types:
            self.process_adaptive_plan(event_data)

    def log_state(self):
        super(Scheduler, self).log_state()
        self._log_dict('Bufferstream to Dataflow', self.bufferstream_to_dataflow)
        self.current_strategy.log_state()

    def run(self):
        super(Scheduler, self).run()
        self.log_state()
        self.cmd_thread = threading.Thread(target=self.run_forever, args=(self.process_cmd,))
        self.data_thread = threading.Thread(target=self.run_forever, args=(self.process_data,))
        self.cmd_thread.start()
        self.data_thread.start()
        self.cmd_thread.join()
        self.data_thread.join()
