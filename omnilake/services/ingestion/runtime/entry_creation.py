'''
Handles the processing of new entries and adds them to the storage.
'''
import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict, List

from da_vinci.core.global_settings import setting_value
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

from omnilake.tables.entries.client import Entry, EntriesClient, EntriesScanDefinition
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


def _validate_uniqueness(content_content_hash: str):
    """
    Validates the uniqueness of the content against the existing entries.

    Keyword arguments:
    content_content_hash -- The content hash to validate
    """
    entries = EntriesClient()

    scan_def = EntriesScanDefinition()

    scan_def.add('content_hash', 'equal', content_content_hash)

    res = entries.full_scan(scan_def)

    if res:
        existing_entry_ids = [entry.entry_id for entry in res]

        raise ValueError(f"Content with hash {content_content_hash} already exists in entries: {existing_entry_ids}")


def _set_source_latest_content_entry_id(entry_effective_date: datetime, entry_id: str, original_of_source: str):
    """
    Sets the latest content entry ID of the source.

    Keyword arguments:
    entry_effective_date -- The effective date of the entry
    entry_id -- The entry ID to set
    original_of_source -- The original source to set the latest content entry ID for
    """
    sources = SourcesClient()

    source_rn = SourceResourceName.from_resource_name(original_of_source)

    source = sources.get(source_type=source_rn.resource_id.source_type, source_id=source_rn.resource_id.source_id)

    if not source:
        raise ValueError(f"Unable to locate source {source_rn}")

    if not source.latest_content_entry_id:
        logging.debug(f"Entry ID not set for latest_content_entry_id .. setting for source {source_rn} to {entry_id}")

        source.latest_content_entry_id = entry_id

    else:
        entries = EntriesClient()

        latest_entry = entries.get(entry_id=source.latest_content_entry_id)

        # Set timezone to UTC before conversion
        if entry_effective_date.tzinfo is None:
            entry_effective_date = entry_effective_date.replace(tzinfo=utc_tz)

        if latest_entry:
            latest_entry_effective_date = latest_entry.effective_on.replace(tzinfo=utc_tz)

            if latest_entry_effective_date < entry_effective_date:
                logging.debug(f"Setting latest entry ID for source {source_rn} to {entry_id}")

                source.latest_content_entry_id = entry_id
            else:
                logging.debug(f"Latest entry {source.latest_content_entry_id} for source {source_rn} is newer than the entry {entry_id} being added")

        else:
            logging.debug(f"Setting latest entry ID for source {source_rn} to {entry_id}")

            source.latest_content_entry_id = entry_id

    sources.put(source)


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

        effective_on = effective_on

        if effective_on:
            effective_on = datetime.fromisoformat(effective_on)

        entry = Entry(
            char_count=len(content),
            content_hash=Entry.calculate_hash(content),
            effective_on=effective_on,
            original_of_source=original_of_source,
            sources=set(sources),
        )

        enforce_content_uniqueness = setting_value('omnilake::ingestion', 'enforce_content_uniqueness')

        if enforce_content_uniqueness:
            content_uniqueness_validation = job.create_child(job_type='CONTENT_UNIQUENESS_VALIDATION')

            jobs.put(job)

            with jobs.job_execution(content_uniqueness_validation, fail_all_parents=True, skip_completion=True):
                _validate_uniqueness(entry.content_hash)

        entries = EntriesClient()

        entries.put(entry)

        storage_mgr = RawStorageManager()

        res = storage_mgr.save_entry(entry_id=entry.entry_id, content=content)

        logging.debug(f"Save entry result: {res}")

        if original_of_source:
            _set_source_latest_content_entry_id(
                entry_effective_date=entry.effective_on,
                entry_id=entry.entry_id,
                original_of_source=original_of_source,
            )

    destination_archive_id = event_body.get("destination_archive_id")

    # If there is an archive ID, send an event to index the entry
    if destination_archive_id:
        event_publisher = EventPublisher()

        index_body = ObjectBody(
            body={
                "archive_id": destination_archive_id,
                "entry_id": entry.entry_id,
                "entry_details": entry.to_dict(json_compatible=True),
                "parent_job_id": job.job_id,
                "parent_job_type": job.job_type,
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
                delay=5 # Delay to give S3 time to catch up
            )