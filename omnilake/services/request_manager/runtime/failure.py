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

from omnilake.services.request_manager.runtime.stage_complete import close_out


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

    originating_event_details = event_body['originating_event_details']

    lake_request_id = originating_event_details['event_body']['lake_request_id']

    # Close the lake request
    lake_requests = LakeRequestsClient()

    lake_request = lake_requests.get(lake_request_id=lake_request_id)

    status_message = None

    # Check for the Da Vinci Bus Response Reason, use that as the status message
    da_vinci_bus_response = event_body.get('da_vinci_event_bus_response')

    if da_vinci_bus_response:
        failure_reason = da_vinci_bus_response.get('reason')

        if failure_reason:
            status_message = failure_reason

    close_out(
        lake_request=lake_request,
        lake_requests_client=lake_requests,
        response_status=LakeRequestStatus.FAILED,
        status_message=status_message,
    )

    logging.info(f'Lake request {lake_request_id} failed')