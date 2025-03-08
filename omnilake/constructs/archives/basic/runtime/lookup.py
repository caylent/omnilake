"""
Handles the lookup of data in a basic archive.
"""

import logging

from typing import Dict, List, Optional

from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from da_vinci.exception_trap.client import ExceptionReporter

from omnilake.internal_lib.event_definitions import (
    LakeRequestLookupResponse,
    LakeRequestInternalRequestEventBodySchema,
)

from omnilake.tables.indexed_entries.client import IndexedEntriesClient, IndexedEntriesScanDefinition
from omnilake.tables.jobs.client import JobsClient


def _lookup_requested_entries(archive_id: str, max_entries: Optional[int] = None,
                                   prioritized_tags: Optional[List[str]] = None) -> List[str]:
    '''
    Loads the inclusive resources

    Keyword arguments:
    archive_id -- The archive ID
    max_entries -- The maximum number of entries to return
    prioritized_tags -- The prioritized tags
    '''
    found_entries = []

    entry_scanner = IndexedEntriesScanDefinition()

    entry_scanner.add('archive_id', 'equal', archive_id)

    entries = IndexedEntriesClient()

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

    return [entr.entry_id for entr in collected_entries]


_FN_NAME = "omnilake.constructs.archives.basic.lookup" 


@fn_event_response(exception_reporter=ExceptionReporter(), function_name=_FN_NAME,
                   logger=Logger(_FN_NAME), handle_callbacks=True)
def basic_lookup(event: Dict, context: Dict):
    '''
    Compacts the content of the resources.
    '''
    logging.debug(f'Received request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=LakeRequestInternalRequestEventBodySchema,
    )

    # Prep for the child job
    parent_job_id = event_body["parent_job_id"]

    parent_job_type = event_body["parent_job_type"]

    jobs_client = JobsClient()

    parent_job = jobs_client.get(job_type=parent_job_type, job_id=parent_job_id, consistent_read=True)

    child_job = parent_job.create_child(job_type="ARCHIVE_BASIC_LOOKUP")

    # Execute the entry lookup under the child job
    with jobs_client.job_execution(child_job, fail_parent=True):

        retrieval_instructions = event_body.get("request_body")

        archive_id = retrieval_instructions.get("archive_id")

        max_entries = retrieval_instructions.get("max_entries")

        prioritize_tags = retrieval_instructions.get("prioritize_tags")

        retrieved_entries = _lookup_requested_entries(
            archive_id=archive_id,
            max_entries=max_entries,
            prioritized_tags=prioritize_tags,
        )

        lake_request_id = event_body.get("lake_request_id")

        response_obj = ObjectBody(
            body={
                "entry_ids": retrieved_entries,
                "lake_request_id": lake_request_id,
            },
            schema=LakeRequestLookupResponse,
        )

        event_publisher = EventPublisher()

        event_publisher.submit(
            event=EventBusEvent(
                body=response_obj.to_dict(ignore_unkown=True),
                event_type=response_obj.get("event_type", strict=True),
            ),
        )