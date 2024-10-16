'''
Handle events for putting an archive into maintenance mode and taking it out of maintenance mode.
'''
import logging

from dataclasses import dataclass
from datetime import datetime, UTC as utc_tz
from typing import Dict


from da_vinci.core.global_settings import setting_value
from da_vinci.core.logging import Logger

from da_vinci.event_bus.client import fn_event_response
from da_vinci.event_bus.event import Event as EventBusEvent

from da_vinci.exception_trap.client import ExceptionReporter

from omnilake.internal_lib.event_definitions import GenericEventBody

from omnilake.tables.archives.client import ArchivesClient, ArchiveStatus
from omnilake.tables.jobs.client import JobsClient, JobStatus

@dataclass
class RequestMaintenanceModeBegin(GenericEventBody):
    archive_id: str
    job_id: str
    job_type: str


@dataclass
class RequestMaintenanceModeEnd(GenericEventBody):
    archive_id: str
    job_id: str
    job_type: str


def job_identifier_str(job_type: str, job_id: str):
    """
    Return a string identifier for a job.

    Keyword arguments:
    job_type -- the type of job
    job_id -- the ID of the job
    """
    return f'{job_type}/{job_id}'


def job_from_identifier_str(identifier: str):
    """
    Return the job type and job ID from a job identifier string.

    Keyword arguments:
    identifier -- the job identifier string
    """
    return identifier.split('/')


@fn_event_response(exception_reporter=ExceptionReporter(), function_name='end_maintenance_mode',
                     logger=Logger('omnilake.storage.end_maintenance_mode'))
def end_maintenance_mode(event: Dict, context: Dict):
    """
    Event handler for taking an archive out of maintenance mode.
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = RequestMaintenanceModeEnd(**source_event.body)

    archives = ArchivesClient()

    archive = archives.get(archive_id=event_body.archive_id)

    if not archive:
        raise ValueError(f'Archive {event_body.archive_id} not found.')

    job_id_str = job_identifier_str(event_body.job_type, event_body.job_id)

    if job_id_str not in archive.status_context_job_ids:
        logging.info(f'Archive {event_body.archive_id} is not in maintenance mode for {job_id_str}.')

        return

    archive.status_context_job_ids.remove(job_id_str)

    if not archive.status_context_job_ids:
        archive.status = ArchiveStatus.ACTIVE

    archives.updated_on = datetime.now(tz=utc_tz)

    archives.put(archive)

    jobs = JobsClient()

    job_obj = jobs.get(job_type=event_body.job_type, job_id=event_body.job_id)

    # Close out the job if it is still in progress
    if job_obj and job_obj.status == JobStatus.IN_PROGRESS:
        job_obj.status = JobStatus.COMPLETED

        job_obj.ended = datetime.now(tz=utc_tz)

        jobs.put(job_obj)


@fn_event_response(exception_reporter=ExceptionReporter(), function_name='begin_maintenance_mode',
                   logger=Logger('omnilake.storage.begin_maintenance_mode'))
def begin_maintenance_mode(event: Dict, context: Dict):
    """
    Event handler for putting an archive into maintenance mode.
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = RequestMaintenanceModeBegin(**source_event.body)

    archives = ArchivesClient()

    archive = archives.get(archive_id=event_body.archive_id)

    if not archive:
        raise ValueError(f'Archive {event_body.archive_id} not found.')

    job_id_str = job_identifier_str(event_body.job_type, event_body.job_id)

    if job_id_str in archive.status_context_job_ids:
        logging.info(f'Archive {event_body.archive_id} is already in maintenance mode for {job_id_str}.')

        return

    archive.status_context_job_ids.append(job_id_str)

    archive.status = ArchiveStatus.MAINTENANCE

    archives.updated_on = datetime.now(tz=utc_tz)

    archives.put(archive)