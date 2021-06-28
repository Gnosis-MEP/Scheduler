from .base import BaseStrategy


class SingleBestStrategy(BaseStrategy):

    def __init__(self, parent_service):
        super(SingleBestStrategy, self).__init__(parent_service)
        self.bufferstream_to_dataflow = {}

    def update(self, strategy_plan):
        dataflows_dict = strategy_plan['dataflows']
        for buffer_stream_key, dataflows in dataflows_dict.items():
            if 'load_shedding' in strategy_plan['name']:
                self.bufferstream_to_dataflow[buffer_stream_key] = dataflows[0][2]
                self.bufferstream_load_shedding[buffer_stream_key] = dataflows[0][0]
            else:
                self.bufferstream_to_dataflow[buffer_stream_key] = dataflows[0][1]

    def get_bufferstream_dataflow(self, buffer_stream_key):
        return self.bufferstream_to_dataflow.get(buffer_stream_key, [])

    def log_state(self):
        super(SingleBestStrategy, self).log_state()
        self.logger.info(f'Bufferstream to dataflow: {self.bufferstream_to_dataflow}')
