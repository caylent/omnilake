"""
Handle the start of the recursive summarization process.
"""
import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict

from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.event_definitions import (
    LakeRequestInternalRequestEventBodySchema,
)

from omnilake.tables.jobs.client import JobsClient, JobStatus

from omnilake.constructs.processors.recursive_summarization.runtime.failure import FAILURE_EVENT_TYPE

from omnilake.constructs.processors.recursive_summarization.runtime.event_definitions import (
    SummarizationRequestSchema,
)

from omnilake.constructs.processors.recursive_summarization.tables.summary_jobs.client import (
    SummaryJob,
    SummaryJobsTableClient,
)


_FN_NAME = "omnilake.constructs.processors.recursive_summarization.start"


@fn_event_response(exception_reporter=ExceptionReporter(), function_name=_FN_NAME,
                   logger=Logger(_FN_NAME), handle_callbacks=True)
def handler(event: Dict, context: Dict):
    '''
    Summarizes the content of the resources.
    '''
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=LakeRequestInternalRequestEventBodySchema,
    )

    omni_jobs = JobsClient()

    job = omni_jobs.get(job_id=event_body.get("parent_job_id"), job_type=event_body.get("parent_job_type"),
                        consistent_read=True)

    summarization_omni_job = job.create_child(job_type="LAKE_PROCESSOR_SUMMARIZER")

    summarization_omni_job.started = datetime.now(tz=utc_tz)

    summarization_omni_job.status = JobStatus.IN_PROGRESS

    omni_jobs.put(summarization_omni_job)

    entries = event_body["entry_ids"]

    summary_jobs = SummaryJobsTableClient()

    summary_req_body = event_body["request_body"]

    summary_job = SummaryJob(
        configuration=summary_req_body.to_dict(),
        goal=summary_req_body["goal"],
        parent_job_id=summarization_omni_job.job_id,
        parent_job_type=summarization_omni_job.job_type,
        lake_request_id=event_body["lake_request_id"],
        remaining_processes=len(entries),
    )

    summary_jobs.put(summary_job)

    event_publisher = EventPublisher()

    for entry in entries:
        obj_body = ObjectBody(
            body={
                "entry_ids": [entry],
                "goal": summary_req_body["goal"],
                "include_source_metadata": summary_req_body.get("include_source_metadata"),
                "model_id": summary_req_body.get("model_id"),
                "parent_job_id": summary_job.parent_job_id,
                "parent_job_type": summary_job.parent_job_type,
                "prompt": summary_req_body.get("prompt"),
                "summary_request_id": summary_job.summary_request_id,
            },
            schema=SummarizationRequestSchema,
        )

        event_publisher.submit(
            event=source_event.next_event(
                event_type=obj_body["event_type"],
                body=obj_body,
                callback_event_type_on_failure=FAILURE_EVENT_TYPE,
            )
        )