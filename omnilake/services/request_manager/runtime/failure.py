"""
Catch all lake request failures
"""
import logging

from datetime import datetime, UTC as utc_tz

from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.event_bus.client import fn_event_response
from da_vinci.event_bus.event import Event as EventBusEvent

from da_vinci.exception_trap.client import ExceptionReporter

from omnilake.tables.jobs.client import JobsClient, JobStatus
from omnilake.tables.lake_requests.client import (
    LakeRequestsClient,
    LakeRequestStatus,
)

CALLBACK_ON_FAILURE_EVENT_TYPE = 'omnilake_lake_request_failure'


_FN_NAME = 'omnilake.services.request_manager.lake_request_failure'


@fn_event_response(function_name=_FN_NAME, exception_reporter=ExceptionReporter(), logger=Logger(_FN_NAME))
def handler(event, context):
    """
    Catch all lake request failures
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body
    )

    originating_event_details = event_body.get('originating_event_details')

    if not originating_event_details:
        raise ValueError(f'originating_event_details not found in event body: {event_body.to_dict()}')

    lake_request_id = originating_event_details.get('lake_request_id')

    # Close the lake request
    lake_requests = LakeRequestsClient()

    lake_request = lake_requests.get(lake_request_id=lake_request_id)

    lake_request.request_status = LakeRequestStatus.FAILED

    lake_requests.put(lake_request)

    # Close out the job
    jobs = JobsClient()

    request_job = jobs.get(job_id=lake_request.job_id, job_type=lake_request.job_type)

    request_job.status = JobStatus.FAILED

    request_job.ended = datetime.now(tz=utc_tz)

    jobs.put(request_job)