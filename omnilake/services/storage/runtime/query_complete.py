import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict, List

from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.event_definitions import (
    InformationRequestBody,
    QueryCompleteBody,
)
from omnilake.internal_lib.naming import EntryResourceName

from omnilake.tables.entries.client import Entry, EntriesClient
from omnilake.tables.jobs.client import JobsClient, JobStatus
from omnilake.tables.vector_store_queries.client import VectorStoreQueryClient


class EntrySortContainer:
    """
    Sort entry container model.
    """
    def __init__(self, entry: Entry):
        self.entry = entry

    @staticmethod
    def calculate_tag_match_percentage(object_tags: List[str], target_tags: List[str]) -> int:
        """
        Calculate the match percentage between the object's tags and the target tags.

        Keyword arguments:
        object_tags -- The list of tags to compare
        target_tags -- The list of tags to compare
        """
        matching_tags = set(object_tags) & set(target_tags)

        # Calculate the match percentage
        return len(matching_tags) / len(target_tags) * 100

    def calculate_score(self, target_tags: List[str]) -> int:
        """
        Calculate the score based on the target tags.

        Keyword arguments:
        target_tags -- The target tags to calculate the score against.
        """
        tag_score = self.calculate_tag_match_percentage(
            object_tags=self.entry.tags,
            target_tags=target_tags,
        )

        # TODO: Add additional scoring logic here, effective date, etc.

        return tag_score

def sort_resource_names(resource_names: List[EntryResourceName], target_tags: List[str]) -> List[EntryResourceName]:
    """
    Sort the entries based on the target tags.

    Keyword arguments:
    entries -- The entries to sort.
    target_tags -- The target tags to sort against.
    """
    entries = EntriesClient()

    entries_to_sort = []

    for resource_name in resource_names:
        logging.debug(f'Fetching entry ID: {resource_name.resource_id}')

        entry = entries.get(entry_id=resource_name.resource_id)

        logging.debug(f'Entry: {entry}')

        if not entry:
            raise ValueError(f'Could not find entry {resource_name.resource_id}')

        entries_to_sort.append(EntrySortContainer(entry))

    sorted_entries = sorted(
        entries_to_sort,
        key=lambda entry_sort_container: entry_sort_container.calculate_score(target_tags),
        reverse=True,
    )

    result = [EntryResourceName(entry_sort_container.entry.entry_id) for entry_sort_container in sorted_entries]

    return result


@fn_event_response(exception_reporter=ExceptionReporter(), function_name='query_complete',
                     logger=Logger('omnilake.services.storage.query_complete'))
def handler(event: Dict, context: Dict):
    """
    Handle the query complete event
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = QueryCompleteBody(**source_event.body)

    vs_queries = VectorStoreQueryClient()

    vs_queries.add_resulting_resources(
        query_id=event_body.query_id,
        resulting_resources=event_body.resource_names,
    )

    query_info = vs_queries.get(event_body.query_id, consistent_read=True)

    if query_info.remaining_processes > 0:
        logging.info(f'Query {event_body.query_id} still has {query_info.remaining_processes} processes remaining.')

        return

    logging.info(f'Query {event_body.query_id} has completed.')

    query_info.completed_on = datetime.now(tz=utc_tz)

    vs_queries.put(query_info)

    resulting_resources = query_info.resulting_resources

    # Handle sorting of the responses if over the max_entries limit
    if query_info.max_entries < len(query_info.resulting_resources):
        logging.info(f'Sorting results for query {event_body.query_id}.')

        sorted_resources = sort_resource_names(
            resource_names=[EntryResourceName(r) for r in query_info.resulting_resources],
            target_tags=query_info.target_tags,
        )

        resulting_resources = sorted_resources[:query_info.max_entries]

        vs_queries.put(query_info)

    event_publisher = EventPublisher()

    information_request_body = InformationRequestBody(
        request_id=query_info.request_id,
        resource_names=list(resulting_resources),
        request_stage='QUERY_COMPLETE',
    )

    logging.debug(f'Submitting event: {information_request_body}')

    event_publisher.submit(
        event=EventBusEvent(
            body=information_request_body.to_dict(),
            event_type=InformationRequestBody.event_type,
        )
    )

    jobs = JobsClient()

    logging.info(f'Updating job {query_info.job_type}/{query_info.job_id} to COMPLETED.')

    job = jobs.get(job_type=query_info.job_type, job_id=query_info.job_id)

    job.status = JobStatus.COMPLETED

    job.ended = datetime.now(tz=utc_tz)

    jobs.put(job)

    logging.info(f'Job {job.job_type}/{job.job_id} has completed.')