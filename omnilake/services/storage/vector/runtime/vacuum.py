import logging

from typing import Dict

import lancedb

from da_vinci.core.global_settings import setting_value
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.tables.archive_entries.client import ArchiveEntriesClient
from omnilake.tables.jobs.client import Job, JobsClient
from omnilake.tables.vector_stores.client import VectorStoresClient
from omnilake.tables.vector_store_chunks.client import VectorStoreChunksClient

from omnilake.services.storage.vector.runtime.event_definitions import VectorArchiveVacuum


def delete_entry_index(entry_id: str, archive_id: str):
    """
    Delete the entry index for the given entry ID and archive ID.

    Keyword arguments:
    entry_id -- The ID of the entry to delete
    archive_id -- The ID of the archive to delete the entry from
    """
    vector_bucket = setting_value(namespace='vector_storage', setting_key='vector_store_bucket')

    vector_store_chunks = VectorStoreChunksClient()

    chunk_objs = vector_store_chunks.get_chunks_by_archive_and_entry(archive_id, entry_id)

    # Load the vector store bucket information
    db = lancedb.connect(f's3://{vector_bucket}')

    # First organize the chunks, just on the off-chance that there are multiple vector stores involved
    organized_chunks = {}

    # Iterate over the chunks and organize them by vector store ID
    for chunk_obj in chunk_objs:
        if chunk_obj.vector_store_id not in organized_chunks:
            organized_chunks[chunk_obj.vector_store_id] = []

        organized_chunks[chunk_obj.vector_store_id].append(chunk_obj)

    # Iterate over the organized chunks and remove the entries from the vector store
    for vector_store_id, chunks in organized_chunks.items():

        vector_stores = VectorStoresClient()

        vector_store = vector_stores.get(archive_id=archive_id,
                                         vector_store_id=vector_store_id)

        table = db.open_table(name=vector_store.vector_store_name)

        for chunk in chunks:
            from_entry = table.search().where(f"chunk_id == '{chunk.chunk_id}'").to_list()

            if len(from_entry) == 0:
                logging.error(f"Could not find entry for chunk: {chunk.chunk_id}")

                continue

            # Remove the entry from the old vector store
            table.delete(f"chunk_id == '{chunk.chunk_id}'")

            # Update the vector store chunk table
            vector_store_chunks.delete(chunk)

        vector_store.total_entries -= 1

        vector_stores.put(vector_store)


@fn_event_response(exception_reporter=ExceptionReporter(), function_name='vector_vacuum',
                   logger=Logger('omnilake.storage.vector.vector_vacuum'))
def handler(event: Dict, context: Dict):
    """
    Lambda handler for the vector vacuum function. This function is responsible for vacuuming the vector store
    to determine and remove entries that are no longer needed.
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = VectorArchiveVacuum(**source_event.body)

    jobs = JobsClient()

    with jobs.job_execution(Job(job_type='VECTOR_VACUUM')):
        logging.debug(f"Deleting entry index for entry {event_body.entry_id} in archive {event_body.archive_id}")

        delete_entry_index(event_body.entry_id, event_body.archive_id)

        archive_entries_client = ArchiveEntriesClient()

        archive_entry = archive_entries_client.get(entry_id=event_body.entry_id, archive_id=event_body.archive_id)

        if archive_entry:

            archive_entries_client.delete(archive_entry)

            logging.debug(f"Deleted entry index for entry {event_body.entry_id} in archive {event_body.archive_id}")

        else:
            logging.debug(f"Could not find entry index for entry {event_body.entry_id} in archive {event_body.archive_id} ... nothing to delete")