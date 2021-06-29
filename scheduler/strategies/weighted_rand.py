import random
from .base import BaseStrategy


class WeightedRandomStrategy(BaseStrategy):

    def __init__(self, parent_service):
        super(WeightedRandomStrategy, self).__init__(parent_service)
        self.bufferstream_to_dataflow_choices = {}
        self.bufferstream_load_shedding_choices = {}
        self.has_load_shedding = False

    def update(self, strategy_plan):
        dataflows = strategy_plan['dataflows']
        self.has_load_shedding = 'load_shedding' in strategy_plan['name']
        self.bufferstream_to_dataflow_choices = dataflows

    def get_bufferstream_dataflow(self, buffer_stream_key):
        return self.get_weighted_random_buffer_stream_dataflow(buffer_stream_key)

    def get_weighted_random_buffer_stream_dataflow(self, buffer_stream_key):
        zipped_dataflow_weighted_choices = self.bufferstream_to_dataflow_choices.get(buffer_stream_key, [])
        if len(zipped_dataflow_weighted_choices) == 0:
            return zipped_dataflow_weighted_choices

        if self.has_load_shedding:
            cum_weights, load_shedding_choices, dataflow_choices = list(zip(*zipped_dataflow_weighted_choices))
        else:
            cum_weights, dataflow_choices = list(zip(*zipped_dataflow_weighted_choices))

        selected_choice_index = random.choices(range(len(dataflow_choices)), cum_weights=cum_weights, k=1)[0]
        single_choice = dataflow_choices[selected_choice_index]
        if self.has_load_shedding:
            load_shedding_rate = load_shedding_choices[selected_choice_index]
            if self.is_shedding_event(load_shedding_rate):
                single_choice = None
        return single_choice

    def log_state(self):
        super(WeightedRandomStrategy, self).log_state()
        self.logger.info(f'Bufferstream to dataflow choices: {self.bufferstream_to_dataflow_choices}')
