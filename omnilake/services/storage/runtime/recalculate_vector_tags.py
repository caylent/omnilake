'''
Recalculate vector tags for vector stores that have not been recalculated in a while.
'''
import logging

from dataclasses import dataclass
from datetime import datetime, UTC as utc_tz
from typing import Dict

from da_vinci.core.global_settings import setting_value
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.event_definitions import (
    GenericEventBody,
    VectorStoreTagRecalculation,
)
from omnilake.internal_lib.job_types import JobType

from omnilake.tables.entries.client import EntriesClient
from omnilake.tables.jobs.client import Job, JobsClient
from omnilake.tables.vector_stores.client import VectorStoresClient
from omnilake.tables.vector_store_tags.client import VectorStoreTagsClient


@dataclass
class VectorStoreTagRecalculationFinished(GenericEventBody):
    parent_job_id: str
    parent_job_type: str


@fn_event_response(exception_reporter=ExceptionReporter(), function_name='recalculate_vector_tags',
                   logger=Logger('omnilake.storage.recalculate_vector_tags'))
def handler(event: Dict, context: Dict):
    """
    Lambda handler for recalculate_vector_tags.
    """
    logging.debug(f'Recieved request: {event}')

    # Import localized to avoid issues with other functions load times
    import lancedb

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = VectorStoreTagRecalculation(**source_event.body)

    jobs_client = JobsClient()

    parent_job = None

    job = Job(job_type=JobType.RECALCULATE_VECTOR_TAGS)

    with jobs_client.job_execution(job, failure_status_message='Failed to recalculate vector tags'):
        # Load the vector store DynamoDB Object
        vector_stores = VectorStoresClient()

        vector_store = vector_stores.get(archive_id=event_body.archive_id, vector_store_id=event_body.vector_store_id)

        vector_bucket = setting_value(namespace='storage', setting_key='vector_store_bucket')

        # Connect to the vector store bucket
        db = lancedb.connect(f's3://{vector_bucket}')

        table = db.open_table(name=vector_store.vector_store_name)

        all_inuse_tags = set()

        entries = EntriesClient()

        final_entry_count = 0

        processed_entries = []

        # Fetch all entries from the table
        for chunk in table.search().limit(None).to_list():
            entry_id = chunk['entry_id']

            if entry_id in processed_entries:
                logging.debug(f'Skipping already processed entry: {entry_id}')

                continue

            logging.debug(f'Processing entry: {entry_id}')

            processed_entries.append(entry_id)

            entry_obj = entries.get(entry_id=entry_id)

            all_inuse_tags.update(entry_obj.tags)

            final_entry_count += 1

        logging.debug(f'All inuse tags: {all_inuse_tags}')

        vector_store_tags_client = VectorStoreTagsClient()

        existing_tags = vector_store_tags_client.get_tags_for_vector_store(
            archive_id=event_body.archive_id,
            vector_store_id=event_body.vector_store_id,
        )

        logging.debug(f'Existing tags found: {existing_tags}')

        all_existing_tags = set(existing_tags)

        unused_tags = all_existing_tags - all_inuse_tags

        logging.debug(f'Unused tags identified: {unused_tags}')

        if unused_tags:
            logging.info(f'Removing unused tags: {unused_tags}')

            vector_store_tags_client.remove_vector_store_from_tags(
                archive_id=event_body.archive_id,
                vector_store_id=event_body.vector_store_id,
                tags=list(unused_tags),
            )

        # Save the vector store with 
        vector_stores = VectorStoresClient()

        vector_store = vector_stores.get(archive_id=event_body.archive_id, vector_store_id=event_body.vector_store_id)

        vector_store.total_entries = final_entry_count

        vector_store.total_entries_last_calculated = datetime.now(tz=utc_tz)

        vector_stores.put(vector_store)

    logging.debug(f'Job completed: {job.job_id}')

    if not parent_job:
        return