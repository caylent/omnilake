import logging

from typing import Dict

import lancedb

from da_vinci.core.global_settings import setting_value
from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.tables.indexed_entries.client import IndexedEntriesClient
from omnilake.tables.jobs.client import Job, JobsClient
from omnilake.constructs.archives.vector.tables.vector_stores.client import VectorStoresClient
from omnilake.constructs.archives.vector.tables.vector_store_chunks.client import VectorStoreChunksClient

from omnilake.constructs.archives.vector.runtime.event_definitions import VectorArchiveVacuumSchema


def delete_entry_index(entry_id: str, archive_id: str):
    """
    Delete the entry index for the given entry ID and archive ID.

    Keyword arguments:
    entry_id -- The ID of the entry to delete
    archive_id -- The ID of the archive to delete the entry from
    """
    vector_bucket = setting_value(namespace='omnilake::vector_storage', setting_key='vector_store_bucket')

    vector_store_chunks = VectorStoreChunksClient()

    chunk_objs = vector_store_chunks.get_chunks_by_archive_and_entry(archive_id, entry_id)

    # Load the vector store bucket information
    db = lancedb.connect(f's3://{vector_bucket}')

    # Iterate over the organized chunks and remove the entries from the vector store
    vector_stores = VectorStoresClient()

    vector_store = vector_stores.get(
        archive_id=archive_id,
    )

    table = db.open_table(name=vector_store.vector_store_id)

    for chunk in chunk_objs:
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


_FN_NAME = 'omnilake.constructs.vector.vector_vacuum'

@fn_event_response(exception_reporter=ExceptionReporter(), function_name=_FN_NAME,
                   logger=Logger(namespace=_FN_NAME))
def handler(event: Dict, context: Dict):
    """
    Lambda handler for the vector vacuum function. This function is responsible for vacuuming the vector store
    to determine and remove entries that are no longer needed.
    """
    logging.debug(f'Received request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=VectorArchiveVacuumSchema,
    )

    jobs = JobsClient()

    archive_id = event_body.get('archive_id')

    entry_id = event_body.get('entry_id')

    # TODO: Find all previous entries that are no longer needed and remove them and decrement the total_entries count

    with jobs.job_execution(Job(job_type='VECTOR_VACUUM')):
        logging.debug(f"Deleting entry index for entry {entry_id} in archive {archive_id}")

        delete_entry_index(entry_id, archive_id)

        archive_entries_client = IndexedEntriesClient()

        archive_entry = archive_entries_client.get(entry_id=entry_id, archive_id=archive_id)

        if archive_entry:
            archive_entries_client.delete(archive_entry)

            logging.debug(f"Deleted entry index for entry {entry_id} in archive {archive_id}")

        else:
            logging.debug(f"Could not find entry index for entry {entry_id} in archive {archive_id} ... nothing to delete")