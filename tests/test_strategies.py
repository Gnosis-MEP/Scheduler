from unittest import TestCase
from unittest.mock import patch, MagicMock

from scheduler.strategies.weighted_rand import WeightedRandomStrategy


class TestWeightedRandomStrategy(TestCase):

    def test_process_cmd_should_call_process_action(self):
        self.strategy = WeightedRandomStrategy(parent_service=MagicMock())
        bf_key = 'bf-key'
        self.strategy.bufferstream_to_dataflow_choices = {
            bf_key: [
                [0.1, [['object-detection-ssd-data'], ['wm-data']]],
                [0.2, [['object-detection-ssd-gpu-data'], ['wm-data']]]
            ]
        }

        dataflow = self.strategy.get_bufferstream_dataflow(bf_key)
        expected_dataflow = self.strategy.bufferstream_to_dataflow_choices[bf_key][0][1]
        expected_dataflow2 = self.strategy.bufferstream_to_dataflow_choices[bf_key][1][1]
        try:
            self.assertListEqual(expected_dataflow, dataflow)
        except:
            self.assertListEqual(expected_dataflow2, dataflow)
