import random

class BaseStrategy():

    def __init__(self, parent_service):
        self.parent_service = parent_service
        self.logger = self.parent_service.logger

    def update(self, strategy_plan):
        raise NotImplementedError()

    def get_bufferstream_dataflow(self, buffer_stream_key):
        raise NotImplementedError()

    def is_shedding_event(self, load_shedding_rate):
        if load_shedding_rate is None or load_shedding_rate == 0:
            return False
        shed_roll = random.randint(0, 100) / 100
        return shed_roll <= load_shedding_rate

    def log_state(self):
        self.logger.info(f'Strategy: {self.__class__.__name__}')
