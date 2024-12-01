'''
Handles the processing of new entries and adds them to the storage.
'''
import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict, List

from da_vinci.core.global_settings import setting_value
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.ai import AI, ModelIDs, AIInvocationResponse
from omnilake.internal_lib.clients import RawStorageManager
from omnilake.internal_lib.event_definitions import (
    AddEntryBody,
    IndexBasicEntryBody,
    IndexVectorEntryBody,
)
from omnilake.internal_lib.job_types import JobType
from omnilake.internal_lib.naming import (
    OmniLakeResourceName,
    EntryResourceName,
    SourceResourceName,
)

from omnilake.tables.archives.client import ArchivesClient
from omnilake.tables.entries.client import Entry, EntriesClient, EntriesScanDefinition
from omnilake.tables.jobs.client import JobsClient, JobStatus
from omnilake.tables.sources.client import SourcesClient


def _summarize_content(content: str) -> AIInvocationResponse:
    """
    Creates a summary of the given content

    Keyword arguments:
    content -- The content to summarize
    """
    ai = AI()

    result = ai.invoke(
        model_id=ModelIDs.SONNET,
        prompt=f"""Summarize the given content concisely, ensuring that:

- All important details are preserved
- Direct quotations are included and properly attributed
- Cited sources are mentioned
- Key arguments, findings, and conclusions are captured
- The summary is significantly shorter than the original text
- No new information is added
- Only provide a summary of the content without any additional commentary or analysis

Aim for clarity and brevity while maintaining the essence and accuracy of the original content.

CONTENT:
{content}""",
    )

    return result


class SourceValidateException(Exception):
    def __init__(self, resource_name: str, reason: str):
        super().__init__(f"Unable to validate source existence for \"{resource_name}\": {reason}")


def _validate_sources(sources: List[str], original_source: str = None):
    """
    Validates the sources.

    Keyword arguments:
    sources -- The sources to validate
    """
    entries_tbl = EntriesClient()

    sources_tbl = SourcesClient()

    if original_source:
        logging.debug(f"Validating original source: {original_source}")

        source_rn = SourceResourceName.from_resource_name(original_source)

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


def _set_source_latest_content_entry_id(entry_effective_date: datetime, entry_id: str, original_source: str):
    """
    Sets the latest content entry ID of the source.

    Keyword arguments:
    entry_effective_date -- The effective date of the entry
    entry_id -- The entry ID to set
    original_source -- The original source to set the latest content entry ID for
    """
    sources = SourcesClient()

    source_rn = SourceResourceName.from_resource_name(original_source)

    source = sources.get(source_type=source_rn.resource_id.source_type, source_id=source_rn.resource_id.source_id)

    if not source:
        raise ValueError(f"Unable to locate source {source_rn}")

    if not source.latest_content_entry_id:
        logging.debug(f"Entry ID not set .. setting latest entry ID for source {source_rn} to {entry_id}")

        source.latest_content_entry_id = entry_id

    else:
        entries = EntriesClient()

        latest_entry = entries.get(entry_id=source.latest_content_entry_id)

        # Set timezone to UTC before conversion
        if entry_effective_date.tzinfo is None:
            entry_effective_date = entry_effective_date.replace(tzinfo=utc_tz)

        latest_entry_effective_date = latest_entry.effective_on.replace(tzinfo=utc_tz)

        if latest_entry:
            if latest_entry_effective_date < entry_effective_date:
                logging.debug(f"Setting latest entry ID for source {source_rn} to {entry_id}")

                source.latest_content_entry_id = entry_id
            else:
                logging.debug(f"Latest entry {source.latest_content_entry_id} for source {source_rn} is newer than the entry {entry_id} being added")

        else:
            logging.debug(f"Setting latest entry ID for source {source_rn} to {entry_id}")

            source.latest_content_entry_id = entry_id

    sources.put(source)


@fn_event_response(function_name='add_entry_processor', exception_reporter=ExceptionReporter(), logger=Logger("omnilake.ingestion.new_entry_processor"))
def handler(event: Dict, context: Dict):
    """
    Processes the new entries and adds them to the storage.
    """
    source_event = EventBusEvent.from_lambda_event(event)

    event_body = AddEntryBody(**source_event.body)

    jobs = JobsClient()

    job = jobs.get(job_type=event_body.job_type, job_id=event_body.job_id)

    # If the job has not started, set the start time
    if not job.started:
        job.started = datetime.now(tz=utc_tz)

    job.status = JobStatus.IN_PROGRESS

    source_validation_job = job.create_child(job_type='SOURCE_VALIDATION')

    jobs.put(job)

    # Cause the parent job to fail if the source validation fails
    with jobs.job_execution(job, failure_status_message='Failed to process entry',
                            skip_initialization=True, skip_completion=True):

        with jobs.job_execution(source_validation_job, failure_status_message='Failed to validate sources'):
            _validate_sources(event_body.sources, event_body.original_source)

        jobs.put(job)

        effective_on = event_body.effective_on

        if effective_on:
            effective_on = datetime.fromisoformat(event_body.effective_on)

        entry = Entry(
            char_count=len(event_body.content),
            content_hash=Entry.calculate_hash(event_body.content),
            effective_on=effective_on,
            original_of_source=event_body.original_source,
            sources=set(event_body.sources),
        )

        enforce_content_uniqueness = setting_value('ingestion', 'enforce_content_uniqueness')

        if enforce_content_uniqueness:
            content_uniqueness_validation = job.create_child(job_type='CONTENT_UNIQUENESS_VALIDATION')

            jobs.put(job)

            with jobs.job_execution(content_uniqueness_validation, fail_all_parents=True, skip_completion=True):
                _validate_uniqueness(entry.content_hash)

        entries = EntriesClient()

        entries.put(entry)

        storage_mgr = RawStorageManager()

        res = storage_mgr.save_entry(entry_id=entry.entry_id, content=event_body.content)

        logging.debug(f"Save entry result: {res}")

        if event_body.original_source:
            _set_source_latest_content_entry_id(
                entry.effective_on,
                entry.entry_id,
                event_body.original_source,
            )

    if event_body.summarize:
        summarize_job = job.create_child(job_type='SUMMARIZE_ENTRY')

        jobs.put(job)

        with jobs.job_execution(summarize_job):
            summary_results = _summarize_content(event_body.content)

            event_publisher = EventPublisher()

            event_publisher.submit(
                event=source_event.next_event(
                    event_type=AddEntryBody.event_type,
                    body=AddEntryBody(
                        archive_id=event_body.archive_id,
                        content=summary_results.response,
                        effective_on=event_body.effective_on,
                        job_id=event_body.job_id,
                        sources=[
                            str(EntryResourceName(entry.entry_id))
                        ]
                    ).to_dict()
                )
            )

    # If there is an archive ID, send an event to index the entry
    if event_body.archive_id:
        event_publisher = EventPublisher()

        index_body_args = {
            'archive_id': event_body.archive_id,
            'entry_id': entry.entry_id,
            'effective_on': event_body.effective_on,
            'job_id': event_body.job_id,
            'job_type':JobType.ADD_ENTRY,
            'original_of_source': event_body.original_source,
        }

        archives = ArchivesClient()

        archive = archives.get(archive_id=event_body.archive_id)

        archive_type = archive.storage_type

        if archive_type == 'BASIC':
            event_publisher.submit(
                event=source_event.next_event(
                    event_type=IndexBasicEntryBody.event_type,
                    body=IndexBasicEntryBody(**index_body_args).to_dict()
                ),
                delay=5 # Delay to give S3 time to catch up
            )

        elif archive_type == 'VECTOR':
            event_publisher.submit(
                event=source_event.next_event(
                    event_type=IndexVectorEntryBody.event_type,
                    body=IndexVectorEntryBody(**index_body_args).to_dict()
                ),
                delay=5 # Delay to give S3 time to catch up
            )

        else:
            raise ValueError(f"Unsupported archive type: {archive_type}")