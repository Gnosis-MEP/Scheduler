from .base import BaseStrategy


class RoundRobinStrategy(BaseStrategy):

    def __init__(self, parent_service):
        super(RoundRobinStrategy, self).__init__(parent_service)
        self.bufferstream_to_dataflow_choices = {}
        self.bufferstream_dataflow_last_index = {}

    def update(self, strategy_plan):
        dataflows = strategy_plan['dataflows']
        self.bufferstream_to_dataflow_choices = dataflows
        for bufferstream in self.bufferstream_to_dataflow_choices.keys():
            if bufferstream not in self.bufferstream_dataflow_last_index.keys():
                self.bufferstream_dataflow_last_index[bufferstream] = 0

    def get_bufferstream_dataflow(self, buffer_stream_key):
        return self.get_round_robin_buffer_stream_dataflow(buffer_stream_key)

    def get_round_robin_buffer_stream_dataflow(self, buffer_stream_key):
        zipped_dataflow_weighted_choices = self.bufferstream_to_dataflow_choices.get(buffer_stream_key, [])
        if len(zipped_dataflow_weighted_choices) == 0:
            return zipped_dataflow_weighted_choices
        cum_weights, dataflow_choices = list(zip(*zipped_dataflow_weighted_choices))

        next_dataflow_index = self.bufferstream_dataflow_last_index.get(buffer_stream_key, 0)
        if next_dataflow_index >= len(dataflow_choices):
            next_dataflow_index = 0
        next_dataflow = dataflow_choices[next_dataflow_index]
        next_dataflow_index += 1
        self.bufferstream_dataflow_last_index[buffer_stream_key] = next_dataflow_index
        return next_dataflow

    def log_state(self):
        super(RoundRobinStrategy, self).log_state()
        self.logger.info(f'Bufferstream to dataflow choices: {self.bufferstream_to_dataflow_choices}')
        self.logger.debug(f'Bufferstream to next round robin selection: {self.bufferstream_dataflow_last_index}')
