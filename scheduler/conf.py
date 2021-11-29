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

LISTEN_EVENT_TYPE_NEW_QUERY_SCHEDULING_PLANNED = config('LISTEN_EVENT_TYPE_NEW_QUERY_SCHEDULING_PLANNED')

SERVICE_CMD_KEY_LIST = [
    LISTEN_EVENT_TYPE_NEW_QUERY_SCHEDULING_PLANNED,
]

# PUB_EVENT_TYPE_NEW_QUERY_SCHEDULING_PLANNED = config('PUB_EVENT_TYPE_NEW_QUERY_SCHEDULING_PLANNED')

PUB_EVENT_LIST = [
    # PUB_EVENT_TYPE_NEW_QUERY_SCHEDULING_PLANNED,
]


SERVICE_DETAILS = None

DEFAULT_SCHEDULING_STRATEGY = config('DEFAULT_SCHEDULING_STRATEGY', default='self-adaptive')


LOGGING_LEVEL = config('LOGGING_LEVEL', default='DEBUG')
