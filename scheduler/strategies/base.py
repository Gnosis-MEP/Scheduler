class BaseStrategy():

    def __init__(self, parent_service):
        self.parent_service = parent_service
        self.logger = self.parent_service.logger

    def update(self, strategy_plan):
        raise NotImplementedError()

    def get_bufferstream_dataflow(self, buffer_stream_key):
        raise NotImplementedError()

    def log_state(self):
        self.logger.info(f'Strategy: {self.__class__.__name__}')
