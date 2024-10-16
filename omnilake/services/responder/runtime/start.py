'''
Handles the initial information request
'''
import logging
import math
import random

from datetime import datetime, UTC as utc_tz
from typing import Dict, List, Optional

from da_vinci.core.logging import Logger
from da_vinci.core.global_settings import setting_value

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from da_vinci.exception_trap.client import ExceptionReporter

from omnilake.internal_lib.event_definitions import (
    InformationRequestBody,
    QueryRequestBody,
)
from omnilake.internal_lib.naming import EntryResourceName

from omnilake.tables.jobs.client import Job, JobsClient, JobStatus
from omnilake.tables.information_requests.client import (
    InformationRequestsClient,
)
from omnilake.tables.compaction_jobs.client import (
    CompactionJob,
    CompactionJobsTableClient
)

from omnilake.tables.vector_store_chunks.client import VectorStoreChunksClient

from omnilake.services.responder.runtime.compactor import CompactionRequest
from omnilake.services.responder.runtime.request_types import (
    load_raw_requests,
    VectorArchiveInformationRequest,
)


def _calculate_expected_recursion_depth(total_entries_count: int) -> int:
    """
    Calculates the expected recursion depth

    Keyword arguments:
    max_entries -- The maximum number of entries
    sample_size_percentage -- The sample size percentage
    """
    max_group_size = setting_value(namespace='responder', setting_key='max_content_group_size')

    expected_depth = 1 # Start at 1 for the initial summarization of each individual entry

    remaining_entries = total_entries_count

    while remaining_entries > 1:
        remaining_entries = math.ceil(remaining_entries / max_group_size)

        expected_depth += 1

    return expected_depth


def _load_resource_names(archive_id: str, sample_size_percentage: int, max_entries: Optional[int] = None) -> List[str]:
    '''
    Loads the resources

    Keyword arguments:
    archive_id -- The archive ID
    sample_size_percentage -- The sample size, percentage of the total number of entries to return
    max_entries -- The maximum number of entries to return
    '''
    vector_chunks = VectorStoreChunksClient()

    params = {
        "KeyConditionExpression": "ArchiveId = :archive_id",
        "ExpressionAttributeValues": {":archive_id": {"S": archive_id}},
    }

    all_archive_entry_ids = set()

    # TODO: This needs to be changed to use a scan against the entries table. Long term, this would be handled by the archive type's service
    for page in vector_chunks.paginated(call="query", parameters=params):
        for chunk in page:
            all_archive_entry_ids.add(chunk.entry_id)

    entry_list_size = len(all_archive_entry_ids)

    real_sample_size = int(entry_list_size * (sample_size_percentage / 100))

    if real_sample_size < 1:
        real_sample_size = 1

    if real_sample_size > entry_list_size:
        real_sample_size = entry_list_size

        sampled_entry_ids = list(all_archive_entry_ids)

    else:
        sampled_entry_ids = random.sample(list(all_archive_entry_ids), real_sample_size)[:max_entries]

    return [str(EntryResourceName(resource_id=entry_id)) for entry_id in sampled_entry_ids]


def _query_request(parent_job: Job, request_id: str, request: VectorArchiveInformationRequest):
    '''
    Handles query requests

    Keyword arguments:
    parent_job -- The parent job
    request_id -- The request ID
    request -- The request
    '''
    event_publisher = EventPublisher()

    query_request = QueryRequestBody(
        archive_id=request.archive_id,
        max_entries=request.max_entries,
        request_id=request_id,
        parent_job_id=parent_job.job_id,
        parent_job_type=parent_job.job_type,
        query_string=request.query_string,
    )

    event_publisher.submit(
        event=EventBusEvent(
            body=query_request.to_dict(),
            event_type=QueryRequestBody.event_type,
        )
    )


def _validate_resource_names(resource_names: List[str]):
    '''
    Validates the resource names

    Keyword arguments:
    resource_names -- The resource names
    '''
    # TODO: Implement this
    pass


def _handle_initial_phase(event_body: InformationRequestBody) -> bool:
    '''
    Handles the initial phase of the information request

    Keyword arguments:
    event_body -- The event body
    '''
    information_requests = InformationRequestsClient()

    info_request = information_requests.get(event_body.request_id)

    jobs = JobsClient()

    parent_job = jobs.get(job_id=info_request.job_id, job_type=info_request.job_type)

    if event_body.resource_names:
        entry_validation_job = parent_job.create_child(job_type='RESOURCE_VALIDATION')

        jobs.put(parent_job)

        validation_job_failure_message = 'Failed to validate resource names'

        with jobs.job_execution(parent_job, failure_status_message=validation_job_failure_message, skip_completion=True):
            with jobs.job_execution(entry_validation_job, failure_status_message=validation_job_failure_message):
                _validate_resource_names(event_body.resource_names)

        info_request.original_sources = set(event_body.resource_names)

    active_queries = 0

    # Initial load of the requests to catch any errors early
    with jobs.job_execution(parent_job, failure_status_message='Failed to load requested data', skip_completion=True):
        loaded_requests = load_raw_requests(event_body.requests)

        for request in loaded_requests:
            if request.evaluation_type == 'EXCLUSIVE':
                if request.request_type == 'BASIC':
                    raise ValueError('Requests to BASIC archives cannot be EXCLUSIVE, only INCLUSIVE')

                _query_request(parent_job=parent_job, request_id=info_request.request_id, request=request)

                active_queries += 1

            else:
                resources = _load_resource_names(
                    archive_id=request.archive_id,
                    sample_size_percentage=request.sample_size_percentage,
                    max_entries=request.max_entries,
                )

                if not info_request.original_sources:
                    info_request.original_sources = set()

                info_request.original_sources.update(resources)

        info_request.remaining_queries = active_queries

        information_requests.put(info_request)

    if active_queries > 0:
        return False

    return True


@fn_event_response(exception_reporter=ExceptionReporter(), function_name="start_responder",
                   logger=Logger("omnilake.responder.start_responder"))
def start_responder(event: Dict, context: Dict):
    '''
    Compacts the content of the resources.
    '''
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = InformationRequestBody(**source_event.body)

    information_requests = InformationRequestsClient()

    if event_body.request_stage == 'INITIAL':
        init_complete = _handle_initial_phase(event_body)

        if not init_complete:
            return

    if event_body.request_stage == 'QUERY_COMPLETE':
        remaining_queries = information_requests.add_query_results(
            request_id=event_body.request_id,
            results=event_body.resource_names
        )

        if remaining_queries > 0:
            logging.info(f'Remaining queries: {remaining_queries}')

            return

    info_req = information_requests.get(event_body.request_id, consistent_read=True)

    jobs = JobsClient()

    parent_job = jobs.get(job_id=info_req.job_id, job_type=info_req.job_type)

    maximum_recursion_depth = setting_value(namespace="responder", setting_key="compaction_maximum_recursion_depth")

    logging.info(f'Maximum recursion depth: {maximum_recursion_depth}')

    total_num_of_entries = len(info_req.original_sources)

    # Calculate recursion depth in order to determine if the job will exceed the maximum recursion depth, if it does, we fail the job
    expected_recursion_depth = _calculate_expected_recursion_depth(total_num_of_entries)

    logging.info(f'Expected recursion depth: {expected_recursion_depth}')

    if expected_recursion_depth > maximum_recursion_depth:
        parent_job.status = JobStatus.FAILED

        parent_job.status_message = f'Expected recursion depth of {expected_recursion_depth} exceeds the maximum of {maximum_recursion_depth}'

        parent_job.ended = datetime.now(tz=utc_tz)

        jobs.put(parent_job)

    compaction_context = CompactionJob(
        current_run=1,
        request_id=info_req.request_id,
        parent_job_id=info_req.job_id,
        parent_job_type=info_req.job_type,
        remaining_processes=total_num_of_entries,
    )

    compaction_jobs = CompactionJobsTableClient()

    compaction_jobs.put(compaction_context)

    event_publisher = EventPublisher()

    # Yeah, I did do this on purpose :laughing:
    for og_source in info_req.original_sources:
        logging.info(f'Compacting resource: {og_source}')

        event_publisher.submit(
            event=EventBusEvent(
                body=CompactionRequest(
                    goal=event_body.goal,
                    request_id=event_body.request_id,
                    resource_names=[og_source],
                    parent_job_id=parent_job.job_id,
                    parent_job_type=parent_job.job_type,
                ).to_dict(),
                event_type='begin_compaction',
            )
        )