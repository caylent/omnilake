"""
Handle final response
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
    LakeRequestInternalResponseEventBodySchema,
    LakeRequestInternalRequestEventBodySchema,
)
from omnilake.internal_lib.naming import EntryResourceName

from omnilake.tables.provisioned_archives.client import ArchivesClient
from omnilake.tables.entries.client import EntriesClient
from omnilake.tables.jobs.client import JobsClient
from omnilake.tables.registered_request_constructs.client import (
    RegisteredRequestConstructsClient,
    RequestConstructType,
)


def _get_content(entry_id: str, storage_manager: RawStorageManager) -> str:
    '''
    Gets the content

    Keyword arguments:
    entry_id -- The entry ID
    '''
    content_resp = storage_manager.get_entry(entry_id=entry_id)

    if content_resp.status_code >= 400:
        raise ValueError(f"Entry content for ID {entry_id} could not be retrieved.")

    content = content_resp.response_body.get('content')

    if not content:
        raise ValueError(f"Entry with ID {entry_id} is empty.")

    return content


def _get_index_endpoint(archive_id: str) -> str:
    """
    Gets the index endpoint for the archive.

    Keyword arguments:
    archive_id -- The archive ID
    """
    archives = ArchivesClient()

    archive = archives.get(archive_id=archive_id)

    if not archive:
        raise ValueError(f"Unable to locate archive {archive_id}")

    archive_type = archive.archive_type

    registered_constructs = RegisteredRequestConstructsClient()

    registered_construct = registered_constructs.get(
        registered_construct_type=RequestConstructType.ARCHIVE,
        registered_type_name=archive_type,
    )

    if not registered_construct:
        raise ValueError(f"No registered construct for archive type {archive_id}")

    return registered_construct.get_operation_event_name(operation="index")


_FN_NAME = "omnilake.constructs.responders.wrap.response"


@fn_event_response(exception_reporter=ExceptionReporter(), logger=Logger(_FN_NAME), function_name=_FN_NAME,
                   handle_callbacks=True)
def final_responder(event: Dict, context: Dict) -> None:
    """
    Final responder function
    """
    logging.debug(f'Received request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=LakeRequestInternalRequestEventBodySchema,
    )

    jobs = JobsClient()

    parent_job = jobs.get(job_type=event_body["parent_job_type"], job_id=event_body["parent_job_id"])

    final_resp_job = parent_job.create_child(job_type="CONSTRUCT_RESPONDER_WRAP_FINAL_RESPONSE")

    jobs.put(parent_job)

    entries = event_body["entry_ids"]

    with jobs.job_execution(final_resp_job):
        logging.debug(f'Processing final response: {event_body}')

        entry_id = entries[0]

        raw_storage = RawStorageManager()

        processed_content = _get_content(entry_id=entry_id, storage_manager=raw_storage)

        response_config = event_body["request_body"]

        separator = response_config.get("separator")

        final_response = processed_content

        prepend_text = response_config.get("prepend_text")

        if prepend_text:
            final_response = f"{prepend_text}{separator}{final_response}"

        append_text = response_config.get("append_text")

        if append_text:
            final_response = f"{final_response}{separator}{append_text}"

        resp = raw_storage.create_entry(
            content=final_response,
            effective_on=datetime.now(tz=utc_tz).isoformat(),
            sources=[str(EntryResourceName(entry_id))]
        )

        final_entry_id = resp.response_body["entry_id"]

        logging.debug(f'Raw storage response: {resp}')

        event_publisher = EventPublisher()

        publish = ObjectBody(
            body={
                "lake_request_id": event_body.get("lake_request_id"),
                "entry_ids": [final_entry_id],
            },
            schema=LakeRequestInternalResponseEventBodySchema,
        )

        event_publisher.submit(
            event=source_event.next_event(
                event_type=publish.get("event_type"),
                body=publish.to_dict()
            ),
        )

    logging.debug(f'Final response job completed: {final_resp_job.job_id}')

    response_config = event_body.get("response_config", {})

    destination_archive_id = response_config.get("destination_archive_id")

    # Index the entry if a destination archive ID is provided
    if destination_archive_id:
        entries = EntriesClient()

        entry = entries.get(entry_id=entry_id)

        index_body = ObjectBody(
            body={
                "archive_id": destination_archive_id,
                "entry_id": entry_id,
                "entry_details": entry.to_dict(json_compatible=True),
                "parent_job_id": parent_job.job_id,
                "parent_job_type": parent_job.job_type,
            },
            schema=IndexEntryEventBodySchema,
        )

        logging.debug(f"Indexing entry {entry.entry_id} for archive {destination_archive_id}: {index_body.to_dict()}")

        event_type = _get_index_endpoint(archive_id=destination_archive_id)  

        event_publisher.submit(
            event=source_event.next_event(
                event_type=event_type,
                body=index_body.to_dict()
            ),
        )