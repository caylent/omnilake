"""
Watches for summary returns
"""
import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict

from da_vinci.core.global_settings import setting_value
from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.event_definitions import (
    LakeRequestInternalResponseEventBodySchema,
)

from omnilake.tables.jobs.client import JobsClient, JobStatus

# Local imports
from omnilake.constructs.processors.recursive_summarization.runtime.event_definitions import (
    SummarizationCompletedSchema,
    SummarizationRequestSchema,
)

from omnilake.constructs.processors.recursive_summarization.runtime.failure import FAILURE_EVENT_TYPE

from omnilake.constructs.processors.recursive_summarization.tables.summary_jobs.client import (
    SummaryJobStatus,
    SummaryJobsTableClient,
)


_FN_NAME = "omnilake.constructs.processors.recursive_summarization.watcher"


@fn_event_response(exception_reporter=ExceptionReporter(), function_name=_FN_NAME, logger=Logger(_FN_NAME))
def handler(event: Dict, context: Dict):
    '''
    Watches for summary events and triggers the summary process.
    '''
    logging.debug(f'Received request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=SummarizationCompletedSchema,
    )

    summary_jobs = SummaryJobsTableClient()

    summary_request_id = event_body["summary_request_id"]

    summarization_job = summary_jobs.add_completed_entry(
        entry_id=event_body["entry_id"],
        summary_request_id=summary_request_id,
    )

    # Add AI invocation if one was returned
    ai_invocation_id = event_body.get("ai_invocation_id")

    if ai_invocation_id:
        summary_jobs.add_ai_invocation(
            summary_request_id=summary_request_id,
            ai_invocation_id=ai_invocation_id,
        )

    logging.debug(f'Added entry {event_body["entry_id"]} to summary job {event_body["summary_request_id"]}.')

    if summarization_job.remaining_processes != 0:
        logging.info(f'Summary job {summary_request_id} has {summarization_job.remaining_processes} remaining processes.')

        return

    if summarization_job.execution_status == SummaryJobStatus.FAILED:
        logging.info(f'Summary job {summary_request_id} has failed ... exiting process.')

        return

    event_bus = EventPublisher()

    # Summarization job has completed all processes
    if len(summarization_job.current_run_completed_entry_ids) == 1:
        logging.info(f'summary job {summarization_job.summary_request_id} has completed all processes.')

        final_body = ObjectBody(
            body={
                "ai_invocation_ids": summarization_job.ai_invocation_ids,
                "entry_ids": list(summarization_job.current_run_completed_entry_ids),
                "lake_request_id": summarization_job.lake_request_id,
            },
            schema=LakeRequestInternalResponseEventBodySchema,
        )

        event_bus.submit(
            event=source_event.next_event(
                body=final_body.to_dict(),
                event_type=final_body["event_type"],
            )
        )

        omni_jobs = JobsClient()

        omni_job = omni_jobs.get(job_id=summarization_job.parent_job_id, job_type=summarization_job.parent_job_type)

        omni_job.status = JobStatus.COMPLETED

        omni_job.completed = datetime.now(tz=utc_tz)

        omni_jobs.put(omni_job)

        logging.info(f'Final response event submitted for summary job {summarization_job.summary_request_id}.')

        summarization_job.execution_status = SummaryJobStatus.COMPLETED

        summary_jobs.put(summarization_job)

        return

    maximum_recursion_depth = setting_value(
        namespace="omnilake::recursive_summarization_construct",
        setting_key="summary_maximum_recursion_depth",
    )

    if summarization_job.current_run > maximum_recursion_depth:
        raise Exception(f'Summary job {summarization_job.summary_request_id} has exceeded the maximum recursion depth.')

    summarization_job.current_run += 1

    latest_completed_resources_lst = list(summarization_job.current_run_completed_entry_ids)

    max_content_group_size = setting_value(
        namespace="omnilake::recursive_summarization_construct",
        setting_key="max_content_group_size",
    )

    # Group the resources into the maximum content group size. Sorry for the lack of readability - Jim
    # Plus it's a fish ... Grouper ... I'll see myself out.
    grouper = lambda lst, n: [lst[i:i + n] for i in range(0, len(lst), n)]

    summary_groups = grouper(latest_completed_resources_lst, max_content_group_size)

    logging.debug(f'Summary groups: {summary_groups}')

    processes = 0

    summarization_job.current_run_completed_entry_ids = set()

    summarization_job.remaining_processes = processes

    summary_jobs.put(summarization_job)

    for group in summary_groups:
        if len(group) == 1:
            logging.debug(f'Group of 1, adding directly to finished resources.')

            summarization_job.current_run_completed_entry_ids.add(group[0])

            continue

        logging.debug(f'Group of {len(group)} resources, submitting for summarization.')

        processes += 1

        request_body = ObjectBody(
            body={
                "effective_on_calculation_rule": summarization_job.configuration.get("effective_on_calculation_rule"),
                "entry_ids": list(group),
                "goal": summarization_job.goal,
                "include_source_metadata": summarization_job.configuration.get("include_source_metadata"),
                "model_id": summarization_job.configuration.get("model_id"),
                "parent_job_id": summarization_job.parent_job_id,
                "parent_job_type": summarization_job.parent_job_type,
                "prompt": summarization_job.configuration.get("prompt"),
                "summary_request_id": summarization_job.summary_request_id,
            },
            schema=SummarizationRequestSchema,
        )

        event_bus.submit(
            event=source_event.next_event(
                body=request_body.to_dict(),
                callback_event_type_on_failure=FAILURE_EVENT_TYPE,
                event_type=request_body.get("event_type"),
            )
        )

        summarization_job.remaining_processes = processes

        summary_jobs.put(summarization_job)