"""
Handle final response
"""
import logging

from typing import Dict

from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.event_definitions import (
    IndexEntryEventBodySchema,
    LakeRequestInternalResponseEventBodySchema,
    LakeRequestInternalRequestEventBodySchema,
)

from omnilake.tables.provisioned_archives.client import ArchivesClient
from omnilake.tables.entries.client import EntriesClient
from omnilake.tables.jobs.client import JobsClient
from omnilake.tables.registered_request_constructs.client import (
    RegisteredRequestConstructsClient,
    RequestConstructType,
)


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


_FN_NAME = "omnilake.constructs.responders.direct.response"


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

    final_resp_job = parent_job.create_child(job_type="CONSTRUCT_RESPONDER_DIRECT_FINAL_RESPONSE")

    jobs.put(parent_job)

    entries = event_body["entry_ids"]

    with jobs.job_execution(final_resp_job):
        logging.debug(f'Processing final response: {event_body}')

        if len(entries) != 1:
            raise ValueError("Only one entry is supported by the DIRECT responder")

        entry_id = entries[0]

        event_publisher = EventPublisher()

        publish = ObjectBody(
            body={
                "lake_request_id": event_body.get("lake_request_id"),
                "entry_ids": [entry_id],
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