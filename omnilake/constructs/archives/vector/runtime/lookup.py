"""
Handles the lookup request for vector archives
"""
import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict

from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent
from omnilake.internal_lib.event_definitions import (
    LakeRequestInternalRequestEventBodySchema,
    LakeRequestLookupResponse,
)

from omnilake.tables.jobs.client import JobsClient, JobStatus

# Local imports
from omnilake.constructs.archives.vector.runtime.query import VectorStorageSearch

from omnilake.constructs.archives.vector.tables.vector_stores.client import VectorStoresClient


_FN_NAME = 'omnilake.constructs.vector.lookup'


@fn_event_response(exception_reporter=ExceptionReporter(), function_name=_FN_NAME,
                   logger=Logger(_FN_NAME), handle_callbacks=True)
def handler(event: Dict, context: Dict):
    """
    Handle the query request

    Keyword arguments:
    event -- The event that triggered the function.
    context -- The context of the function.
    """
    logging.debug(f'Received request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(body=source_event.body, schema=LakeRequestInternalRequestEventBodySchema)

    jobs = JobsClient()

    parent_job_type = event_body.get("parent_job_type")

    parent_job_id = event_body.get("parent_job_id")

    parent_job = jobs.get(job_type=parent_job_type, job_id=parent_job_id)

    query_job = parent_job.create_child(job_type='QUERY_REQUEST')

    jobs.put(parent_job)

    query_job.status = JobStatus.IN_PROGRESS

    query_job.started = datetime.now(tz=utc_tz)

    jobs.put(query_job)

    vs_client = VectorStoresClient()

    lookup_instructions = event_body["request_body"]

    archive_id = lookup_instructions["archive_id"]

    vector_store = vs_client.get(archive_id=archive_id)

    if not vector_store:
        raise ValueError(f'Could not find vector store for archive {archive_id}')
    
    vector_store_search = VectorStorageSearch()

    query_string = lookup_instructions["query_string"]

    max_entries = lookup_instructions.get("max_entries")

    prioritize_tags = lookup_instructions.get("prioritize_tags")

    search_results = vector_store_search.execute(
        archive_id=archive_id,
        query_string=query_string,
        max_entries=max_entries,
        prioritize_tags=prioritize_tags,
    )

    logging.debug(f'Final search results: {search_results}')

    lake_request_id = event_body.get("lake_request_id")

    response_obj = ObjectBody(
        body={
            "entry_ids": search_results,
            "lake_request_id": lake_request_id,
        },
        schema=LakeRequestLookupResponse,
    )

    event_publisher = EventPublisher()

    event_publisher.submit(
        event=EventBusEvent(
            body=response_obj.to_dict(),
            event_type=response_obj.get("event_type", strict=True),
        ),
    )

    query_job.status = JobStatus.COMPLETED

    query_job.ended = datetime.now(tz=utc_tz)

    jobs.put(query_job)