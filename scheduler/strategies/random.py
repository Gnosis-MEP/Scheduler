import random
from .base import BaseStrategy


class RandomStrategy(BaseStrategy):

    def __init__(self, parent_service):
        super(RandomStrategy, self).__init__(parent_service)
        self.bufferstream_to_dataflow_choices = {}

    def update(self, strategy_plan):
        dataflows = strategy_plan['dataflows']
        self.bufferstream_to_dataflow_choices = dataflows

    def get_bufferstream_dataflow(self, buffer_stream_key):
        return self.get_random_buffer_stream_dataflow(buffer_stream_key)

    def get_random_buffer_stream_dataflow(self, buffer_stream_key):
        zipped_dataflow_weighted_choices = self.bufferstream_to_dataflow_choices.get(buffer_stream_key, [])
        if len(zipped_dataflow_weighted_choices) == 0:
            return zipped_dataflow_weighted_choices
        cum_weights, dataflow_choices = list(zip(*zipped_dataflow_weighted_choices))
        selected_choices = random.choices(dataflow_choices, k=1)
        single_choice = selected_choices[0]
        return single_choice

    def log_state(self):
        super(RandomStrategy, self).log_state()
        self.logger.info(f'Bufferstream to dataflow choices: {self.bufferstream_to_dataflow_choices}')
