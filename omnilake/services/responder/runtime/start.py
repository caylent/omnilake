'''
Handles the initial information request
'''
import logging
import math

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
from omnilake.internal_lib.naming import (
    EntryResourceName,
    OmniLakeResourceName,
)

from omnilake.tables.archives.client import ArchivesClient
from omnilake.tables.archive_entries.client import ArchiveEntriesClient, ArchiveEntriesScanDefinition
from omnilake.tables.entries.client import EntriesClient
from omnilake.tables.jobs.client import Job, JobsClient, JobStatus
from omnilake.tables.information_requests.client import (
    InformationRequestsClient,
)
from omnilake.tables.sources.client import SourcesClient
from omnilake.tables.summary_jobs.client import (
    SummaryJob,
    SummaryJobsTableClient,
)

from omnilake.services.responder.runtime.summarizer import SummarizationRequest
from omnilake.services.responder.runtime.request_types import (
    load_raw_requests,
    VectorInformationRetrievalRequest,
)


def _calculate_expected_recursion_depth(total_entries_count: int) -> int:
    """
    Calculates the expected recursion depth

    Keyword arguments:
    max_entries -- The maximum number of entries
    """
    max_group_size = setting_value(namespace='responder', setting_key='max_content_group_size')

    expected_depth = 1 # Start at 1 for the initial summarization of each individual entry

    remaining_entries = total_entries_count

    while remaining_entries > 1:
        remaining_entries = math.ceil(remaining_entries / max_group_size)

        expected_depth += 1

    return expected_depth


def _expand_source_resource_names(resource_names: List[str]) -> List[str]:
    '''
    Expands the source resource names and finds the latest content entry for the source

    Keyword arguments:
    resource_names -- The resource names
    '''
    expanded_resources = []

    sources = SourcesClient()

    for resource_name in resource_names:
        resource = OmniLakeResourceName.from_string(resource_name)

        if resource.resource_type == 'source':
            source = sources.get(source_type=resource.resource_id.source_type, source_id=resource.resource_id.source_id)

            if source:
                expanded_resources.append(EntryResourceName(resource_id=source.latest_content_entry_id))

            else:
                raise ValueError(f'Source with ID {resource.resource_id.source_id} does not exist')

        else:
            expanded_resources.append(resource_name)

    return expanded_resources


def _load_inclusive_resource_names(archive_id: str, max_entries: Optional[int] = None, prioritized_tags: Optional[List[str]] = None) -> List[str]:
    '''
    Loads the inclusive resources

    Keyword arguments:
    archive_id -- The archive ID
    max_entries -- The maximum number of entries to return
    prioritized_tags -- The prioritized tags
    '''
    found_entries = []

    entry_scanner = ArchiveEntriesScanDefinition()

    entry_scanner.add('archive_id', 'equal', archive_id)

    entries = ArchiveEntriesClient()

    for page in entries.scanner(entry_scanner):
        for entry in page:
            found_entries.append(entry)

    entry_list_size = len(found_entries)

    if not max_entries:
        max_entries = 1

    if max_entries < entry_list_size:
        collected_entries = sorted(
            found_entries,
            key=lambda entry_obj: entry_obj.calculate_score(prioritized_tags),
            reverse=True,
        )[:max_entries]

    else:
        collected_entries = found_entries

    return [str(EntryResourceName(resource_id=entr.entry_id)) for entr in collected_entries]


def _query_request(parent_job: Job, request_id: str, request: VectorInformationRetrievalRequest):
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
        prioritize_tags=request.prioritize_tags,
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
    entries = EntriesClient()

    for resource_name in resource_names:
        resource = OmniLakeResourceName.from_string(resource_name)

        logging.info(f'Validating resource name: {resource}')

        entry = entries.get(resource.resource_id)

        if not entry:
            raise ValueError(f'Entry with resource name {resource_name} does not exist')


def _validate_requests(requests: List[VectorInformationRetrievalRequest]):
    '''
    Validates the request

    Keyword arguments:
    request -- The request
    '''
    archives = ArchivesClient()

    fetched_archives = []

    for request in requests:
        if request.request_type == 'RELATED':
            # Skip related requests, we assume they are valid
            continue

        archive_id = request.archive_id

        if archive_id in fetched_archives:
            continue

        archive = archives.get(archive_id=archive_id)

        if not archive:
            raise ValueError(f'Archive with ID {archive_id} does not exist')

        fetched_archives.append(archive_id)


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

        explicit_resource_names = _expand_source_resource_names(event_body.resource_names)

        with jobs.job_execution(entry_validation_job, failure_status_message=validation_job_failure_message, fail_all_parents=True):
            _validate_resource_names(explicit_resource_names)

        info_request.original_sources = set(explicit_resource_names)

    active_queries = 0

    loaded_requests = load_raw_requests(event_body.retrieval_requests)

    with jobs.job_execution(parent_job, skip_completion=True):
        _validate_requests(loaded_requests)

    # Initial load of the requests to catch any errors early
    with jobs.job_execution(parent_job, failure_status_message='Failed to load requested data', skip_completion=True):
        loaded_requests = load_raw_requests(event_body.retrieval_requests)

        loaded_archive_information = {} # Hash of arhives already looked up to prevent duplicate lookups

        archives_client = ArchivesClient()

        # Set the original sources to an empty set if it is not already set
        if not info_request.original_sources:
            info_request.original_sources = set()

        for request in loaded_requests:
            if request.request_type == 'VECTOR':
                if request.archive_id in loaded_archive_information:
                    loaded_archive = loaded_archive_information[request.archive_id]

                else:
                    loaded_archive = archives_client.get(archive_id=request.archive_id)

                    loaded_archive_information[request.archive_id] = loaded_archive

                # Validate that the archive is a VECTOR archive
                if loaded_archive.storage_type != 'VECTOR':
                    raise ValueError('Cannot perform exclusive requests on non-VECTOR archives')

                _query_request(parent_job=parent_job, request_id=info_request.request_id, request=request)

                active_queries += 1

            elif request.request_type == 'RELATED':
                # Fetch the information for the related requests
                related_request = information_requests.get(request_id=request.related_request_id)

                info_request.original_sources.update(related_request.original_sources)

            else:
                resources = _load_inclusive_resource_names(
                    archive_id=request.archive_id,
                    max_entries=request.max_entries,
                    prioritized_tags=request.prioritize_tags,
                )

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

    maximum_recursion_depth = setting_value(namespace="responder", setting_key="summary_maximum_recursion_depth")

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

    compaction_context = SummaryJob(
        current_run=1,
        request_id=info_req.request_id,
        parent_job_id=info_req.job_id,
        parent_job_type=info_req.job_type,
        remaining_processes=total_num_of_entries,
    )

    compaction_jobs = SummaryJobsTableClient()

    compaction_jobs.put(compaction_context)

    event_publisher = EventPublisher()

    # Submit the initial summarization requests for each source
    for og_source in info_req.original_sources:
        logging.info(f'Summarizing resource: {og_source}')

        event_publisher.submit(
            event=source_event.next_event(
                body=SummarizationRequest(
                    goal=event_body.goal,
                    include_source_metadata=info_req.include_source_metadata,
                    request_id=event_body.request_id,
                    resource_names=[og_source],
                    parent_job_id=parent_job.job_id,
                    parent_job_type=parent_job.job_type,
                ).to_dict(),
                event_type='begin_summarization',
            )
        )