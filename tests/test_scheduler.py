from unittest.mock import patch

from event_service_utils.tests.base_test_case import MockedServiceStreamTestCase
from event_service_utils.tests.json_msg_helper import prepare_event_msg_tuple

from scheduler.service import Scheduler

from scheduler.conf import (
    SERVICE_STREAM_KEY,
    SERVICE_CMD_KEY,
    EVENT_DISPATCHER_STREAM_KEY
)


class TestScheduler(MockedServiceStreamTestCase):
    GLOBAL_SERVICE_CONFIG = {
        'service_stream_key': SERVICE_STREAM_KEY,
        'service_cmd_key': SERVICE_CMD_KEY,
        'event_dispatcher_data_key': EVENT_DISPATCHER_STREAM_KEY,
        'logging_level': 'ERROR',
        'tracer_configs': {'reporting_host': None, 'reporting_port': None},
    }
    SERVICE_CLS = Scheduler
    MOCKED_STREAMS_DICT = {
        SERVICE_STREAM_KEY: [],
        SERVICE_CMD_KEY: [],
    }

    @patch('scheduler.service.Scheduler.process_action')
    def test_process_cmd_should_call_process_action(self, mocked_process_action):
        action = 'someAction'
        event_data = {
            'id': 1,
            'action': action,
            'some': 'stuff'
        }
        msg_tuple = prepare_event_msg_tuple(event_data)
        mocked_process_action.__name__ = 'process_action'

        self.service.service_cmd.mocked_values = [msg_tuple]
        self.service.process_cmd()
        self.assertTrue(mocked_process_action.called)
        self.service.process_action.assert_called_once_with(action=action, event_data=event_data, json_msg=msg_tuple[1])

    @patch('scheduler.service.Scheduler.execute_adaptive_plan')
    def test_process_action_should_call_process_action_executeAdaptivePlan(self, mocked_execute_plan):
        action = 'executeAdaptivePlan'
        dataflow = {
        }

        event_data = {
            'id': 1,
            'action': action,
            'dataflow': dataflow
        }
        msg_tuple = prepare_event_msg_tuple(event_data)
        self.service.process_action(action, event_data, msg_tuple[1])

        self.assertTrue(mocked_execute_plan.called)
        mocked_execute_plan.assert_called_once_with(
            dataflow
        )

    @patch('scheduler.service.Scheduler.apply_dataflow_to_event')
    @patch('scheduler.service.Scheduler.send_event_to_dispatcher')
    def test_process_data_event_should_call_send_event_to_dispatcher(self, mocked_send_dispatcher, mocked_apply_dataflow):
        event_data = {
            'id': 1,
            'buffer_stream_key': 'buffer-key',
        }

        event_data_with_dataflow = event_data.copy()
        event_data_with_dataflow.update({
            'data_flow': ['service1-stream', 'service2-stream'],
            'data_path': []
        })

        mocked_apply_dataflow.return_value = event_data_with_dataflow
        msg_tuple = prepare_event_msg_tuple(event_data)

        self.service.process_data_event(event_data, msg_tuple[1])

        self.assertTrue(mocked_send_dispatcher.called)
        mocked_send_dispatcher.assert_called_once_with(
            event_data_with_dataflow
        )

    def test_apply_dataflow_to_event_should_add_correct_fields(self):
        event_data = {
            'id': 1,
            'buffer_stream_key': 'buffer-stream-key1',
        }
        self.service.bufferstream_to_dataflow = {
            'buffer-stream-key1': ['service-stream1', 'service-stream2'],
            'buffer-stream-key2': ['service-stream3', 'service-stream4'],
        }

        altered_event = self.service.apply_dataflow_to_event(event_data)
        self.assertIn('data_flow', altered_event)
        self.assertIn('data_path', altered_event)
        self.assertEquals(['service-stream1', 'service-stream2'], altered_event['data_flow'])
        self.assertEquals([], altered_event['data_path'])
