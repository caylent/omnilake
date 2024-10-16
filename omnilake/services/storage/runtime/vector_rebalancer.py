import logging

from dataclasses import dataclass
from datetime import datetime, UTC as utc_tz
from typing import Dict, List

import lancedb

from da_vinci.core.global_settings import setting_value
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.event_definitions import (
    GenericEventBody,
    IndexEntryBody,
    VectorStoreTagRecalculation,
)

from omnilake.tables.entries.client import Entry, EntriesClient
from omnilake.tables.jobs.client import Job, JobsClient, JobStatus
from omnilake.tables.vector_stores.client import VectorStore, VectorStoresClient
from omnilake.tables.vector_store_tags.client import VectorStoreTagsClient
from omnilake.tables.vector_store_chunks.client import VectorStoreChunksClient

from omnilake.services.storage.runtime.maintenance import (
    RequestMaintenanceModeBegin,
    RequestMaintenanceModeEnd,
)

from omnilake.services.storage.runtime.vector_storage import DocumentChunk, calculate_tag_match_percentage


@dataclass
class VectorStoreRebalancing(GenericEventBody):
    archive_id: str
    vector_store_id: str


def create_new_vector_store(archive_id: str, vector_bucket: str) -> VectorStore:
    """
    Create a new vector store for the given archive.

    Keyword arguments:
    archive_id -- The ID of the archive to create the vector
    vector_bucket -- The bucket to store the vector store
    """
    logging.debug(f"Creating new vector store for archive: {archive_id}")

    vector_stores = VectorStoresClient()

    db = lancedb.connect(f's3://{vector_bucket}')

    new_vector_store = VectorStore(
        archive_id=archive_id,
        bucket_name=vector_bucket,
    )

    db.create_table(
        name=new_vector_store.vector_store_name,
        schema=DocumentChunk,
    )

    vector_stores.put(new_vector_store)

    logging.debug(f"Created new vector store: {new_vector_store.vector_store_id}")

    return new_vector_store


def find_matching_entries(entry_search_list: List[Entry], target_tags: List[str]) -> List[str]:
    """
    Find entries that match the target tags. Returns the list of entry IDs that match

    Keyword arguments:
    entry_search_list -- The list of entries to search
    target_tags -- The list of tags to match
    """
    match_threshold = setting_value(namespace='storage', setting_key='rebalance_tag_match_threshold_percentage')

    matching_entries = []

    for entry in entry_search_list:
        match_percentage = calculate_tag_match_percentage(entry.tags, target_tags)

        logging.debug(f"Match percentage for entry {entry.entry_id}: {match_percentage}")

        if match_percentage >= match_threshold:
            matching_entries.append(entry.entry_id)

    return matching_entries


@fn_event_response(exception_reporter=ExceptionReporter(), function_name='vector_rebalancer',
                   logger=Logger('omnilake.storage.vector_rebalancer'))
def handler(event: Dict, context: Dict):
    """
    Lambda handler for rebalancing a vector store.
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = VectorStoreRebalancing(**source_event.body)

    jobs_client = JobsClient()

    job_type = 'VECTOR_REBALANCING'

    job = Job(
        job_type=job_type,
        started=datetime.now(tz=utc_tz),
        status=JobStatus.IN_PROGRESS,
    )

    jobs_client.put(job)

    chunks = VectorStoreChunksClient()

    all_chunks_for_old_vector = chunks.get_by_vector_store_id(archive_id=event_body.archive_id,
                                                              vector_store_id=event_body.vector_store_id)

    # Cache all the chunk Ids for the old vector store
    entries = {}

    for chunk in all_chunks_for_old_vector:
        logging.debug(f"Found chunk: {chunk.chunk_id}")

        if chunk.entry_id not in entries:
            logging.debug(f"Creating new entry list for entry: {chunk.entry_id}")

            entries[chunk.entry_id] = []

        entries[chunk.entry_id].append(chunk)

    entries_client = EntriesClient()

    all_vector_entry_objs = []

    # Grab all the entries for the old vector store
    for entry_id in entries:
        entry = entries_client.get(entry_id=entry_id)

        all_vector_entry_objs.append(entry)

    # Get the most common tags for the vector store to be moved

    tags_client = VectorStoreTagsClient()

    top_tags_percentage = setting_value(namespace='storage', setting_key='rebalance_top_tags_percentage')

    results = tags_client.get_top_n_percent_tags(
        archive_id=event_body.archive_id,
        vector_store_id=event_body.vector_store_id,
        percentage=top_tags_percentage,
    )

    tag_names = [tag.tag for tag in results]

    logging.debug(f"Found the following top tags: {tag_names}")

    entries_to_move = find_matching_entries(all_vector_entry_objs, tag_names)

    logging.debug(f"Found the following entries to move: {entries_to_move}")

    if len(entries_to_move) == 0:
        logging.info(f"No matching entries found to move for vector store: {event_body.vector_store_id}")

        job.status = JobStatus.COMPLETED

        job.ended = datetime.now(tz=utc_tz)

        jobs_client.put(job)

        return

    logging.debug(f"Creating new vector store for archive: {event_body.archive_id}")

    event_mgr = EventPublisher()

    event_mgr.submit(
        event=EventBusEvent(
            body=RequestMaintenanceModeBegin(
                archive_id=event_body.archive_id,
                job_id=job.job_id,
                job_type=job_type,
            ).to_dict(),
            event_type='begin_maintenance_mode',
        )
    )

    vector_bucket = setting_value(namespace='storage', setting_key='vector_store_bucket')

    new_vector_store = create_new_vector_store(event_body.archive_id, vector_bucket)

    db = lancedb.connect(f's3://{vector_bucket}')

    vector_stores = VectorStoresClient()

    original_vector_store = vector_stores.get(archive_id=event_body.archive_id,
                                              vector_store_id=event_body.vector_store_id)

    original_table = db.open_table(name=original_vector_store.vector_store_name)

    logging.debug(f"Moving entries to new vector store: {new_vector_store.vector_store_id}")

    # Move the entries to the new vector store
    for entry_id in entries_to_move:
        chunks_to_move = entries[entry_id]

        for chunk in chunks_to_move:
            from_entry = original_table.search().where(f"chunk_id == '{chunk.chunk_id}'").to_list()

            if len(from_entry) == 0:
                logging.error(f"Could not find entry for chunk: {chunk.chunk_id}")

                continue

            # Remove the entry from the old vector store
            original_table.delete(f"chunk_id == '{chunk.chunk_id}'")

            original_vector_store.total_entries -= 1

            # Delete the old chunk, new chunks will be created during entry indexing 
            chunks.delete(chunk)

        entry_copy_job = Job(type='REBALANCE_ENTRY')

        jobs_client.put(entry_copy_job)

        event_mgr.submit(
            event=EventBusEvent(
                event_type=IndexEntryBody.event_type,
                body=IndexEntryBody(
                    archive_id=event_body.archive_id,
                    entry_id=entry.entry_id,
                    job_id=entry_copy_job.job_id,
                    job_type=entry_copy_job.job_type,
                    vector_store_id=new_vector_store.vector_store_id,
                ).to_dict()
            ),
        )

        event_mgr.submit(
            event=EventBusEvent(
                body=RequestMaintenanceModeBegin(
                    archive_id=event_body.archive_id,
                    job_id=entry_copy_job.job_id,
                    job_type=entry_copy_job.job_type,
                ).to_dict(),
                event_type='begin_maintenance_mode',
            )
        )

    original_vector_store.total_entries_last_calculated = datetime.now(tz=utc_tz)

    new_vector_store.total_entries_last_calculated = datetime.now(tz=utc_tz)

    vector_stores.put(original_vector_store)

    vector_stores.put(new_vector_store)

    job.status = JobStatus.COMPLETED

    job.ended = datetime.now(tz=utc_tz)

    jobs_client.put(job)

    event_mgr.submit(
        event=EventBusEvent(
            body=RequestMaintenanceModeEnd(
                archive_id=event_body.archive_id,
                job_id=job.job_id,
                job_type=job_type,
            ).to_dict(),
            event_type='end_maintenance_mode',
        )
    )

    logging.info(f'Original vector store "{original_vector_store.archive_id}/{original_vector_store.vector_store_id}" needs tag recalculation')

    event_mgr.submit(
        event=EventBusEvent(
            body=VectorStoreTagRecalculation(
                archive_id=original_vector_store.archive_id,
                vector_store_id=original_vector_store.vector_store_id,
            ).to_dict(),
            event_type='recalculate_vector_tags',
        )
    ) 

    logging.debug(f'Submitting event: {event}')