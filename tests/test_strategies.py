from unittest import TestCase
from unittest.mock import patch, MagicMock

from scheduler.strategies.weighted_rand import WeightedRandomStrategy
from scheduler.strategies.round_robin import RoundRobinStrategy


class TestWeightedRandomStrategy(TestCase):

    def test_get_bufferstream_dataflow_should_select_one_of_the_two_possible_choices(self):
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


class TestRoundRobinStrategyStrategy(TestCase):

    def test_get_bufferstream_dataflow_should_do_a_round_robin(self):
        self.strategy = RoundRobinStrategy(parent_service=MagicMock())
        bf_key = 'bf-key'
        bf_key2 = 'bf-key2'
        self.strategy.bufferstream_to_dataflow_choices = {
            bf_key: [
                [0, [['object-detection-ssd-data'], ['wm-data']]],
                [0, [['object-detection-ssd-gpu-data'], ['wm-data']]]
            ],
            bf_key2: [
                [0, [['object-detection-ssd-data'], ['wm-data']]],
                [0, [['object-detection-ssd-gpu-data'], ['wm-data']]],
                [0, [['object-detection-ssd-gpu3-data'], ['wm-data']]]
            ],
        }

        bf_1_sc_1_dataflow = self.strategy.get_bufferstream_dataflow(bf_key)
        expected_dataflow = [['object-detection-ssd-data'], ['wm-data']]
        self.assertListEqual(expected_dataflow, bf_1_sc_1_dataflow)

        bf_1_sc_2_dataflow = self.strategy.get_bufferstream_dataflow(bf_key)
        expected_dataflow = [['object-detection-ssd-gpu-data'], ['wm-data']]
        self.assertListEqual(expected_dataflow, bf_1_sc_2_dataflow)

        bf_2_sc_1_dataflow = self.strategy.get_bufferstream_dataflow(bf_key2)
        expected_dataflow = [['object-detection-ssd-data'], ['wm-data']]
        self.assertListEqual(expected_dataflow, bf_2_sc_1_dataflow)

        bf_1_sc_3_dataflow = self.strategy.get_bufferstream_dataflow(bf_key)
        expected_dataflow = [['object-detection-ssd-data'], ['wm-data']]
        self.assertListEqual(expected_dataflow, bf_1_sc_3_dataflow)

        bf_2_sc_2_dataflow = self.strategy.get_bufferstream_dataflow(bf_key2)
        expected_dataflow = [['object-detection-ssd-gpu-data'], ['wm-data']]
        self.assertListEqual(expected_dataflow, bf_2_sc_2_dataflow)

        bf_2_sc_3_dataflow = self.strategy.get_bufferstream_dataflow(bf_key2)
        expected_dataflow = [['object-detection-ssd-gpu3-data'], ['wm-data']]
        self.assertListEqual(expected_dataflow, bf_2_sc_3_dataflow)
