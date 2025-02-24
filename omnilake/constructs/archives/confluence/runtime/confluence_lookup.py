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

from omnilake.constructs.archives.confluence.runtime.confluence_client import ConfluenceClient
from omnilake.internal_lib.event_definitions import (
    LakeRequestLookupResponse,
    LakeRequestInternalRequestEventBodySchema,
)
from datetime import datetime, UTC as utc_tz
from omnilake.tables.indexed_entries.client import IndexedEntriesClient, IndexedEntriesScanDefinition
from omnilake.tables.jobs.client import JobsClient
from omnilake.tables.provisioned_archives.client import ArchivesClient
from omnilake.internal_lib.clients import RawStorageManager


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
    raw_storage = RawStorageManager()

    entry_scanner = IndexedEntriesScanDefinition()

    entry_scanner.add('archive_id', 'equal', archive_id)

    archives_client = ArchivesClient()
    archive = archives_client.get(archive_id)

    confluence_client = ConfluenceClient(
        url=archive.configuration['url'],
        username=archive.configuration['username'],
        api_key=archive.configuration['api_key'],
        space_key=archive.configuration['space_key'])

    pages = confluence_client.list_pages()

    for page in pages:
        entry = raw_storage.create_entry_with_source(
            content=page.content,
            effective_on=datetime.now(tz=utc_tz),
            source_type="web_page_content",
            source_arguments={"page_id": page.page_id},
        )
        found_entries.append(entry)



_FN_NAME = "omnilake.constructs.archives.confluence.lookup"


@fn_event_response(exception_reporter=ExceptionReporter(), function_name=_FN_NAME,
                   logger=Logger(_FN_NAME))
def confluence_lookup(event: Dict, context: Dict):
    '''
    Compacts the content of the resources.
    '''
    logging.debug(f'Recieved request: {event}')

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

    child_job = parent_job.create_child(job_type="ARCHIVE_CONFLUENCE_LOOKUP")

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