"""
Basic Archive Provisioner

This function is responsible for creating a new archive
"""
import logging

from datetime import datetime, UTC as utz_tz
from typing import Dict

from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.event_definitions import ProvisionArchiveEventBodySchema
from omnilake.internal_lib.job_types import JobType

from omnilake.tables.provisioned_archives.client import Archive, ArchivesClient, ArchiveStatus
from omnilake.tables.jobs.client import Job, JobsClient, JobStatus


@fn_event_response(exception_reporter=ExceptionReporter(), function_name='basic_archive_provisioner',
                   logger=Logger('omnilake.storage.basic.provisioner'))
def handler(event: Dict, context: Dict) -> Dict:
    """
    Provisions a new archive
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(body=source_event.body, schema=ProvisionArchiveEventBodySchema)

    job_type = event_body.get("job_type") or JobType.CREATE_ARCHIVE

    job_id = event_body.get("job_id")

    jobs = JobsClient()

    job = jobs.get(job_type=job_type, job_id=job_id)

    if not job:
        job = Job(job_id=job_id, job_type=job_type)

    job.status = JobStatus.IN_PROGRESS

    job.started =  datetime.now(tz=utz_tz)

    jobs.put(job)

    description = event_body.get("description")

    archive_id = event_body.get("archive_id")

    configuration = event_body.get("configuration")

    archive_type = configuration.get("archive_type")

    archive = Archive(
        archive_id=archive_id,
        configuration=configuration.to_dict(),
        description=description,
        status=ArchiveStatus.ACTIVE,
        archive_type=archive_type,
    )

    archives = ArchivesClient()

    archives.put(archive)

    job.status = JobStatus.COMPLETED

    job.ended = datetime.now(tz=utz_tz)

    jobs.put(job)