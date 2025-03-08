"""
Index an entry into an archive.
"""
import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict

from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.clients import RawStorageManager
from omnilake.internal_lib.event_definitions import (
    IndexEntryEventBodySchema,
)
from omnilake.internal_lib.naming import SourceResourceName

from omnilake.tables.provisioned_archives.client import ArchivesClient
from omnilake.tables.indexed_entries.client import (
    IndexedEntry,
    IndexedEntriesClient,
    IndexedEntriesScanDefinition,
)
from omnilake.tables.jobs.client import Job, JobsClient, JobStatus
from omnilake.tables.sources.client import SourcesClient

from omnilake.constructs.archives.basic.runtime.event_definitions import (
    BasicArchiveGenerateEntryTagsEventBodySchema,
)


def is_latest_entry_for_original(source_resource_name: str, entry_id: str) -> bool:
    """
    Validate that the latest entry for the given original source is the entry being processed.

    Keyword arguments:
    source_resource_name -- The source resource name to validate.
    """
    sources_client = SourcesClient()

    source_rn = SourceResourceName.from_resource_name(source_resource_name)

    source = sources_client.get(source_type=source_rn.resource_id.source_type, source_id=source_rn.resource_id.source_id)

    logging.debug(f"Checking if entry {entry_id} is the latest entry for original source {source_resource_name}")

    return source.latest_content_entry_id == entry_id


_FN_NAME = "omnilake.constructs.archives.basic.indexer" 

@fn_event_response(function_name=_FN_NAME, exception_reporter=ExceptionReporter(),
                   logger=Logger(_FN_NAME))
def handler(event: Dict, context: Dict):
    """
    Vectorizes the text data and stores it in vector storage.

    Keyword arguments:
    event -- The event data.
    context -- The context data.
    """
    logging.debug(f'Received request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=IndexEntryEventBodySchema,
    )

    jobs = JobsClient()

    job_type = event_body.get("parent_job_type")

    job_id = event_body.get("parent_job_id")

    if not job_id:
        job = Job(job_type=job_type)

        jobs.put(job)

    else:
        job = jobs.get(job_type=job_type, job_id=job_id)

    archive_id = event_body.get("archive_id")

    archives_client = ArchivesClient()

    archive = archives_client.get(archive_id=archive_id)

    retain_latest_originals_only = archive.configuration.get('retain_latest_originals_only')

    original_of_source = event_body.get("original_of_source")

    entry_id = event_body.get("entry_id")

    if retain_latest_originals_only and original_of_source:
        if is_latest_entry_for_original(original_of_source, entry_id):
            logging.debug(f"Entry {entry_id} is the latest entry for original source {original_of_source} ... continuing indexing")

            scan_def = IndexedEntriesScanDefinition()

            scan_def.add('original_of_source', 'equal', original_of_source) 

            indexed_entries_client = IndexedEntriesClient()

            matching_indexed_entries = indexed_entries_client.full_scan(scan_def)

            for archive_entry in matching_indexed_entries:
                if archive_entry.entry_id == entry_id:
                    logging.debug(f"Skipping processed entry")

                    continue

                indexed_entries_client.delete(archive_entry)

                logging.debug(f"Deleted entry index for entry {entry_id} in archive {archive_entry.archive_id}")

        else:
            logging.debug(f"Entry {entry_id} is not the latest entry for original source {original_of_source} ... skipping indexing")

            job.status = JobStatus.COMPLETED

            job.ended = datetime.now(utc_tz)

            jobs.put(job)

            return

    indexed_entries = IndexedEntriesClient()

    entry_obj = indexed_entries.get(archive_id=archive_id, entry_id=entry_id)

    effective_on = event_body.get("effective_on")

    if not entry_obj:
        entry_obj = IndexedEntry(
            archive_id=archive_id,
            entry_id=entry_id,
            effective_on=datetime.fromisoformat(effective_on),
            original_of_source=original_of_source,
            tags=[],
        )

        indexed_entries.put(entry_obj)

    storage_mgr = RawStorageManager()

    # Retrieve the entry content from the storage manager
    entry_content = storage_mgr.get_entry(entry_id)

    if 'message' in entry_content.response_body:
        raise Exception(f"Error retrieving entry content: {entry_content.response_body['message']}")

    logging.info(f"Sending generate_tags event")

    event_publisher = EventPublisher()

    tags_generate_body = ObjectBody(
        body={
            "archive_id": archive_id,
            "entry_id": entry_id,
            "content": entry_content.response_body['content'],
            "parent_job_id": job.job_id,
            "parent_job_type": job.job_type,
        },
        schema=BasicArchiveGenerateEntryTagsEventBodySchema,
    )

    event_publisher.submit(
        event=EventBusEvent(
            body=tags_generate_body.to_dict(),
            event_type=tags_generate_body.get("event_type"),
        )
    )

    # Parent job is completed during the tag generation process