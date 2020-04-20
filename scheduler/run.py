#!/usr/bin/env python
from event_service_utils.streams.redis import RedisStreamFactory

from scheduler.service import Scheduler

from scheduler.conf import (
    REDIS_ADDRESS,
    REDIS_PORT,
    SERVICE_STREAM_KEY,
    EVENT_DISPATCHER_STREAM_KEY,
    SERVICE_CMD_KEY,
    LOGGING_LEVEL,
    TRACER_REPORTING_HOST,
    TRACER_REPORTING_PORT,
)


def run_service():
    tracer_configs = {
        'reporting_host': TRACER_REPORTING_HOST,
        'reporting_port': TRACER_REPORTING_PORT,
    }
    stream_factory = RedisStreamFactory(host=REDIS_ADDRESS, port=REDIS_PORT)
    service = Scheduler(
        service_stream_key=SERVICE_STREAM_KEY,
        service_cmd_key=SERVICE_CMD_KEY,
        event_dispatcher_data_key=EVENT_DISPATCHER_STREAM_KEY,
        stream_factory=stream_factory,
        logging_level=LOGGING_LEVEL,
        tracer_configs=tracer_configs
    )
    service.run()


def main():
    try:
        run_service()
    except KeyboardInterrupt:
        pass


if __name__ == '__main__':
    main()
