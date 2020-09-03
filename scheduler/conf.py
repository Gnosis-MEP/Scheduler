import os

from decouple import config


def int_or_none(val):
    if val is None:
        return None
    int_val = int(val)
    if int_val > 0:
        return int_val
    return None


SOURCE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SOURCE_DIR)

REDIS_ADDRESS = config('REDIS_ADDRESS', default='localhost')
REDIS_PORT = config('REDIS_PORT', default='6379')

REDIS_MAX_STREAM_SIZE = config('REDIS_MAX_STREAM_SIZE', default=None, cast=int_or_none)

TRACER_REPORTING_HOST = config('TRACER_REPORTING_HOST', default='localhost')
TRACER_REPORTING_PORT = config('TRACER_REPORTING_PORT', default='6831')

SERVICE_STREAM_KEY = config('SERVICE_STREAM_KEY')
SERVICE_CMD_KEY = config('SERVICE_CMD_KEY')

EVENT_DISPATCHER_STREAM_KEY = config('EVENT_DISPATCHER_STREAM_KEY')
SCHEDULING_STRATEGY = config('SCHEDULING_STRATEGY', default='self-adaptive')


LOGGING_LEVEL = config('LOGGING_LEVEL', default='DEBUG')
