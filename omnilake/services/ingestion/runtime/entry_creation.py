'''
Handles the processing of new entries and adds them to the storage.
'''
import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict, List

from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.clients import RawStorageManager
from omnilake.internal_lib.event_definitions import (
    AddEntryEventBodySchema,
    IndexEntryEventBodySchema,
)

from omnilake.internal_lib.naming import (
    OmniLakeResourceName,
    SourceResourceName,
)

from omnilake.tables.entries.client import EntriesClient
from omnilake.tables.jobs.client import JobsClient, JobStatus
from omnilake.tables.provisioned_archives.client import ArchivesClient
from omnilake.tables.registered_request_constructs.client import (
    RegisteredRequestConstructsClient,
    RequestConstructType,
)
from omnilake.tables.sources.client import SourcesClient


class SourceValidateException(Exception):
    def __init__(self, resource_name: str, reason: str):
        super().__init__(f"Unable to validate source existence for \"{resource_name}\": {reason}")


def _validate_sources(sources: List[str], original_of_source: str = None):
    """
    Validates the sources.

    Keyword arguments:
    sources -- The sources to validate
    """
    entries_tbl = EntriesClient()

    sources_tbl = SourcesClient()

    if original_of_source:
        logging.debug(f"Validating original source: {original_of_source}")

        source_rn = SourceResourceName.from_resource_name(original_of_source)

        logging.debug(f"Original source resource name: {source_rn}")

        original_of_source = sources_tbl.get(
            source_type=source_rn.resource_id.source_type,
            source_id=source_rn.resource_id.source_id,
        )

        if not original_of_source:
            raise SourceValidateException(
                resource_name=str(source_rn),
                reason="Unable to locate original source information",
            )

    for source in sources:
        resource_name = OmniLakeResourceName.from_string(source)

        logging.debug(f"Validating source: {resource_name}")

        if resource_name.resource_type == "source":
            src = sources_tbl.get(
                source_type=resource_name.resource_id.source_type,
                source_id=resource_name.resource_id.source_id,
            )

            if not src:
                raise SourceValidateException(
                    resource_name=source,
                    reason="Unable to locate source",
                )

        elif resource_name.resource_type == "entry":
            entry = entries_tbl.get(entry_id=resource_name.resource_id)

            if not entry:
                raise SourceValidateException(
                    resource_name=source,
                    reason="Unable to locate entry"
                )

        else:
            raise SourceValidateException(
                resource_name=source,
                reason="Unsupported resource type, only source and entry are supported sources",
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


_FN_NAME = "omnilake.ingestion.new_entry_processor"


@fn_event_response(function_name=_FN_NAME, exception_reporter=ExceptionReporter(),
                   logger=Logger(namespace=_FN_NAME))
def handler(event: Dict, context: Dict):
    """
    Processes the new entries and adds them to the storage.
    """
    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=AddEntryEventBodySchema,
    )

    jobs = JobsClient()

    job = jobs.get(job_type=event_body.get("job_type"), job_id=event_body.get("job_id"))

    job.started = datetime.now(tz=utc_tz)

    job.status = JobStatus.IN_PROGRESS

    source_validation_job = job.create_child(job_type='SOURCE_VALIDATION')

    jobs.put(job)

    # Cause the parent job to fail if the source validation fails
    with jobs.job_execution(job, failure_status_message='Failed to process entry',
                            skip_initialization=True, skip_completion=True):

        sources = event_body.get("sources")

        content = event_body.get("content")

        effective_on = event_body.get("effective_on")

        original_of_source = event_body.get("original_of_source")

        with jobs.job_execution(source_validation_job, failure_status_message='Failed to validate sources'):
            _validate_sources(sources, original_of_source)

        jobs.put(job)

        storage_mgr = RawStorageManager()

        res = storage_mgr.create_entry(
            content=content,
            effective_on=effective_on,
            original_of_source=original_of_source,
            sources=sources
        )

        logging.debug(f"Create entry result: {res}")

        entry_id = res.response_body["entry_id"]

    destination_archive_id = event_body.get("destination_archive_id")

    # If there is an archive ID, send an event to index the entry
    if destination_archive_id:
        event_publisher = EventPublisher()

        entry_desc = storage_mgr.describe_entry(entry_id=entry_id)

        effective_on_actual = entry_desc.response_body["effective_on"]

        index_body = ObjectBody(
            body={
                "archive_id": destination_archive_id,
                "effective_on": effective_on_actual,
                "entry_id": entry_id,
                "original_of_source": original_of_source,
                "parent_job_id": job.job_id,
                "parent_job_type": job.job_type,
            },
            schema=IndexEntryEventBodySchema,
        )

        logging.debug(f"Indexing entry {entry_id} for archive {destination_archive_id}: {index_body.to_dict()}")

        event_type = _get_index_endpoint(archive_id=destination_archive_id)  

        event_publisher.submit(
                event=source_event.next_event(
                    event_type=event_type,
                    body=index_body.to_dict()
                ),
                delay=5 # Delay to give S3 time to catch up
            )