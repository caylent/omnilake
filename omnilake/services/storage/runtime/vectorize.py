'''
Processes all requests to vectorize text data and store it in vector storage.
'''
import json
import logging
import re

from datetime import datetime, UTC as utc_tz
from typing import Dict, List
from uuid import uuid4

import boto3
import lancedb

from da_vinci.core.global_settings import setting_value
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.clients import RawStorageManager
from omnilake.internal_lib.event_definitions import IndexEntryBody
from omnilake.internal_lib.job_types import JobType

from omnilake.services.storage.runtime.vector_storage import (
    choose_vector_stores,
    DocumentChunk,
)

from omnilake.tables.entries.client import EntriesClient
from omnilake.tables.jobs.client import JobsClient, JobStatus
from omnilake.tables.vector_stores.client import VectorStoresClient
from omnilake.tables.vector_store_chunks.client import VectorStoreChunksClient, VectorStoreChunk
from omnilake.tables.vector_store_tags.client import VectorStoreTagsClient

from omnilake.services.storage.runtime.maintenance import (
    RequestMaintenanceModeEnd,
)


def recursive_text_splitter(text: str, max_chunk_length: int = 1000, overlap: int = 40) -> List[str]:
    '''
    Helper function for chunking text recursively based on the max_chunk_length and overlap.

    Keyword arguments:
    text -- The text to chunk.
    max_chunk_length -- The maximum length of each chunk.
    overlap -- The overlap between chunks.
    '''
    # Initialize result
    result = []

    current_chunk_count = 0

    separator = ['\n', ' ']

    _splits = re.split(f'({separator})', text)

    splits = [_splits[i] + _splits[i + 1] for i in range(1, len(_splits), 2)]

    for i in range(len(splits)):

        if current_chunk_count != 0:

            chunk = ''.join(
                splits[
                    current_chunk_count
                    - overlap : current_chunk_count
                    + max_chunk_length
                ]
            )

        else:
            chunk = ''.join(splits[0:max_chunk_length])

        if len(chunk) > 0:

            result.append(''.join(chunk))

        current_chunk_count += max_chunk_length

    return result


def chunk_text(text: str, max_chunk_length: int = 1000, overlap: int = 40) -> List[str]:
    """
    Chunk text into smaller pieces.

    Keyword Arguments:
    text -- The text to chunk.
    max_chunk_length -- The maximum length of each chunk.
    overlap -- The overlap between chunks.
    """
    return recursive_text_splitter(text, max_chunk_length, overlap)


def get_embeddings(text: str):
    """
    Get embeddings for a given text.

    Keyword arguments:
    text -- The text to get embeddings for.
    """
    bedrock = boto3.client(service_name='bedrock-runtime')
    
    body = json.dumps({
        "inputText": text
    })
    
    response = bedrock.invoke_model(
        modelId="amazon.titan-embed-text-v2:0",
        contentType="application/json",
        accept="application/json",
        body=body
    )
    
    response_body = json.loads(response['body'].read())

    embedding = response_body['embedding']
    
    return embedding


def generate_vector_data(entry_id: str, text_chunks: List[str]) -> List[DocumentChunk]:
    """
    Generate vector data for a given text.

    Keyword arguments:
    entry_id -- The entry ID to associate with the vector data.
    text_chunks -- The text chunks to generate vector data for.
    """
    embeddings = []

    for chunk in text_chunks:
        embedding_results = get_embeddings(chunk)

        embeddings.append(embedding_results)

    data = []

    for chunk, embed in zip(text_chunks, embeddings):
        data.append({
            'entry_id': entry_id,
            'chunk_id': str(uuid4()),
            'vector': embed 
        })

    return data


@fn_event_response(function_name="vectorize_content", exception_reporter=ExceptionReporter(), logger=Logger("omnilake.storage.vectorize_entry"))
def handler(event: Dict, context: Dict):
    """
    Vectorizes the text data and stores it in vector storage.

    Keyword arguments:
    event -- The event data.
    context -- The context data.
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = IndexEntryBody(**source_event.body)

    jobs = JobsClient()

    job = jobs.get(job_type=event_body.job_type, job_id=event_body.job_id)

    vectorize_job = job.create_child(job_type=JobType.INDEX_ENTRY)

    vectorize_job.status = JobStatus.IN_PROGRESS

    vectorize_job.started = datetime.now(utc_tz)

    storage_mgr = RawStorageManager()

    # Retrieve the entry content from the storage manager
    entry_content = storage_mgr.get_entry(event_body.entry_id)

    if 'message' in entry_content.response_body:
        raise Exception(f"Error retrieving entry content: {entry_content.response_body['message']}")

    # Get the max chunk length and overlap from the settings
    max_chunk_length = setting_value(namespace='storage', setting_key='max_chunk_length')

    chunk_overlap = setting_value(namespace='storage', setting_key='chunk_overlap')

    # Chunk the text
    text_chunks = chunk_text(entry_content.response_body['content'], max_chunk_length, chunk_overlap)

    # Generate the vector data
    data = generate_vector_data(event_body.entry_id, text_chunks=text_chunks)

    # Connect to the vector storage
    vector_bucket = setting_value(namespace='storage', setting_key='vector_store_bucket')

    db = lancedb.connect(f's3://{vector_bucket}')

    # Choose the appropriate vector store
    entries = EntriesClient()

    entry_obj = entries.get(event_body.entry_id)

    logging.debug(f"Entry tags: {entry_obj.tags}")

    if event_body.vector_store_id:
        logging.debug(f"Using provided vector store ID: {event_body.vector_store_id}")

        vector_store_id = event_body.vector_store_id

    else:
        vector_store_id = choose_vector_stores(event_body.archive_id, entry_obj.tags)[0]

    logging.info(f"Selected vector store: {vector_store_id}")

    vector_stores = VectorStoresClient()

    vector_store_obj = vector_stores.get(event_body.archive_id, vector_store_id)

    vector_table = db.open_table(name=vector_store_obj.vector_store_name)

    vector_table.add(data)

    chunk_meta_client = VectorStoreChunksClient()

    logging.info(f"Adding {len(data)} chunks to vector store {vector_store_id}")

    for chunk in data:
        chunk_meta = VectorStoreChunk(
            archive_id=event_body.archive_id,
            entry_id=chunk['entry_id'],
            chunk_id=chunk['chunk_id'],
            vector_store_id=vector_store_id,
        )

        chunk_meta_client.put(chunk_meta)

    logging.info(f"Saved {len(data)} chunks to vector store {vector_store_id}")

    # Update the entry information
    entry_obj.archives.update(set([event_body.archive_id]))

    entries.put(entry_obj)

    # Update the vector store stats
    vector_store_obj.total_entries += 1

    vector_store_obj.total_entries_last_calculated = datetime.now(utc_tz)

    vector_stores.put(vector_store_obj)

    new_tags = set(entry_obj.tags)

    vector_store_tags = VectorStoreTagsClient()

    vector_store_tags.add_vector_store_to_tags(
        archive_id=event_body.archive_id,
        vector_store_id=vector_store_id,
        tags=list(new_tags)
    )

    # Update the job statuses and close them out
    vectorize_job.status = JobStatus.COMPLETED

    vectorize_job.ended = datetime.now(utc_tz)

    jobs.put(vectorize_job)

    job.status = JobStatus.COMPLETED

    job.ended = datetime.now(utc_tz)

    jobs.put(job)

    event_publisher = EventPublisher()

    logging.info(f"Vectorization complete for entry {event_body.entry_id} .. sending end maintenance mode request")

    event_publisher.submit(
        event=EventBusEvent(
            body=RequestMaintenanceModeEnd(
                archive_id=event_body.archive_id,
                job_id=job.job_id,
                job_type=job.job_type,
            ).to_dict(),
            event_type='end_maintenance_mode',
        )
    )