'''
Storage Provisioner

This function is responsible for creating a new archive and the initial vector store for that archive.
'''
import logging

from datetime import datetime, UTC as utz_tz
from typing import Dict

import lancedb

from da_vinci.core.global_settings import setting_value
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.event_definitions import CreateArchiveBody
from omnilake.internal_lib.job_types import JobType

from omnilake.tables.archives.client import Archive, ArchivesClient, ArchiveStatus
from omnilake.tables.jobs.client import Job, JobsClient, JobStatus
from omnilake.tables.vector_stores.client import (
    VectorStore,
    VectorStoresClient,
)

from omnilake.services.storage.runtime.vector_storage import DocumentChunk


@fn_event_response(exception_reporter=ExceptionReporter(), function_name='storage_provisioner', logger=Logger('omnilake.storage.provisioner'))
def handler(event: Dict, context: Dict) -> Dict:
    """
    Provisions a new archive, creating the initial vector store and archive record
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = CreateArchiveBody(**source_event.body)

    jobs = JobsClient()

    job = jobs.get(job_type=JobType.CREATE_ARCHIVE, job_id=event_body.job_id)

    if not job:
        job = Job(job_id=event_body.job_id, job_type=JobType.CREATE_ARCHIVE)

    job.status = JobStatus.IN_PROGRESS

    job.started =  datetime.now(tz=utz_tz)

    jobs.put(job)

    vector_bucket = setting_value(namespace='storage', setting_key='vector_store_bucket')

    db = lancedb.connect(f's3://{vector_bucket}')

    archive = Archive(
        archive_id=event_body.archive_id,
        description=event_body.description,
        status=ArchiveStatus.ACTIVE,
        visibility=event_body.visibility,
    )

    initial_vector_store = VectorStore(
        archive_id=event_body.archive_id,
        bucket_name=vector_bucket,
    )

    db.create_table(
        name=initial_vector_store.vector_store_name,
        schema=DocumentChunk,
    )

    vector_stores = VectorStoresClient()

    vector_stores.put(initial_vector_store)

    archives = ArchivesClient()

    archives.put(archive)

    job.status = JobStatus.COMPLETED

    job.ended = datetime.now(tz=utz_tz)

    jobs.put(job)