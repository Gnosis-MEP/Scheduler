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
LISTEN_EVENT_TYPE_SERVICE_WORKER_SLR_PROFILE_PLANNED = config('LISTEN_EVENT_TYPE_SERVICE_WORKER_SLR_PROFILE_PLANNED')
LISTEN_EVENT_TYPE_SERVICE_WORKER_OVERLOADED_PLANNED = config('LISTEN_EVENT_TYPE_SERVICE_WORKER_OVERLOADED_PLANNED')
LISTEN_EVENT_TYPE_SERVICE_WORKER_BEST_IDLE_PLANNED = config('LISTEN_EVENT_TYPE_SERVICE_WORKER_BEST_IDLE_PLANNED')
LISTEN_EVENT_TYPE_UNNECESSARY_LOAD_SHEDDING_PLANNED = config('LISTEN_EVENT_TYPE_UNNECESSARY_LOAD_SHEDDING_PLANNED')

SERVICE_CMD_KEY_LIST = [
    LISTEN_EVENT_TYPE_NEW_QUERY_SCHEDULING_PLANNED,
    LISTEN_EVENT_TYPE_SERVICE_WORKER_SLR_PROFILE_PLANNED,
    LISTEN_EVENT_TYPE_SERVICE_WORKER_OVERLOADED_PLANNED,
    LISTEN_EVENT_TYPE_SERVICE_WORKER_BEST_IDLE_PLANNED,
    LISTEN_EVENT_TYPE_UNNECESSARY_LOAD_SHEDDING_PLANNED,
]

PUB_EVENT_TYPE_SCHEDULING_PLAN_EXECUTED = config('PUB_EVENT_TYPE_SCHEDULING_PLAN_EXECUTED')

PUB_EVENT_LIST = [
    PUB_EVENT_TYPE_SCHEDULING_PLAN_EXECUTED,
]


SERVICE_DETAILS = None

DEFAULT_SCHEDULING_STRATEGY = config('DEFAULT_SCHEDULING_STRATEGY', default='round_robin')


LOGGING_LEVEL = config('LOGGING_LEVEL', default='DEBUG')
