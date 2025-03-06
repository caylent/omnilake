"""
Handles the failure of a summarization event
"""
import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict

from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.exceptions import CALLBACK_ON_FAILURE_EVENT_TYPE

from omnilake.tables.jobs.client import JobsClient, JobStatus

# Local imports
from omnilake.constructs.processors.recursive_summarization.runtime.event_definitions import (
    SummarizationCompletedSchema,
    SummarizationRequestSchema,
)

from omnilake.constructs.processors.recursive_summarization.tables.summary_jobs.client import (
    SummaryJobStatus,
    SummaryJobsTableClient,
)

FAILURE_EVENT_TYPE = "omnilake_processor_summarizer_failure"

_FN_NAME = "omnilake.constructs.processors.recursive_summarization.failure"


@fn_event_response(exception_reporter=ExceptionReporter(), function_name=_FN_NAME, logger=Logger(_FN_NAME))
def handler(event: Dict, context: Dict):
    '''
    Watches for summary event failures, closes the execution and passes the failure on to the manager.
    '''
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(body=source_event.body)

    originating_event_details = event_body.get('originating_event_details')

    original_event_body = ObjectBody(
        body=originating_event_details['event_body'],
        schema=SummarizationRequestSchema,
    )

    summary_request_id = original_event_body["summary_request_id"]

    summary_executions = SummaryJobsTableClient()

    summary_job = summary_executions.get(summary_request_id=summary_request_id)

    summary_job.status = SummaryJobStatus.FAILED

    summary_executions.put(summary_job)

    # Close out the job
    jobs = JobsClient()

    omni_job = jobs.get(job_id=original_event_body["parent_job_id"], job_type=original_event_body["parent_job_type"])

    # Check for the Da Vinci Bus Response Reason, use that as the status message
    da_vinci_bus_response = event_body['da_vinci_event_bus_response']

    if da_vinci_bus_response:
        failure_reason = da_vinci_bus_response.get('reason')

        if failure_reason:
            omni_job.status_message = failure_reason

    omni_job.status = JobStatus.FAILED

    omni_job.ended = datetime.now(tz=utc_tz)

    jobs.put(omni_job)

    # Create a new event body with an updated event body so the
    # request manager can properly handle the lake request failure
    request_event_body = event_body.new(
        additions={
            "originating_event_details": {
                "lake_request_id": summary_job.lake_request_id,
            },
        }
    )

    # Send Event failure to the lake
    publisher = EventPublisher()

    publisher.submit(
        event=source_event.next_event(
            body=request_event_body.to_dict(),
            event_type=CALLBACK_ON_FAILURE_EVENT_TYPE,
        ),
    )