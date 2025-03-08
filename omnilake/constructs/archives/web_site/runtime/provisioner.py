"""
Web Site Archive Provisioner

This function is responsible for creating a new archive
"""
import logging

from datetime import datetime, UTC as utz_tz
from urllib.parse import urljoin
from typing import Dict, List

import requests

from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.event_definitions import ProvisionArchiveEventBodySchema
from omnilake.internal_lib.job_types import JobType

from omnilake.tables.provisioned_archives.client import Archive, ArchivesClient, ArchiveStatus
from omnilake.tables.jobs.client import Job, JobsClient, JobStatus
from omnilake.tables.source_types.client import SourceType, SourceTypesClient

from omnilake.constructs.archives.web_site.runtime.lookup import REQUEST_HEADERS


def _check_test_path(url: str) -> bool:
    """
    Checks if the url is a test path
    """
    try:
        response = requests.get(url=url, headers=REQUEST_HEADERS)

        logging.debug(f"Test Path Response satus: {response.status_code}")

        if response.status_code == 200:
            return True

        else:
            return False

    except requests.RequestException as e:
        return False


def _provision_source_type(name: str, description: str, required_fields: List[str]) -> None:
    """
    Provisions the source type if it does not already exist
    """
    source_types = SourceTypesClient()

    existing = source_types.get(source_type_name=name)

    if existing:
        logging.debug(f"Source type {name} already exists")

        return

    source_type = SourceType(
        source_type_name=name,
        required_fields=required_fields,
        description=description,
    )

    source_types.put(source_type)


_FN_NAME = "omnilake.constructs.archives.web_site.provisioner"


@fn_event_response(exception_reporter=ExceptionReporter(), function_name=_FN_NAME, logger=Logger(_FN_NAME))
def handler(event: Dict, context: Dict) -> Dict:
    """
    Provisions a new archive
    """
    logging.debug(f'Received request: {event}')

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

    base_url = configuration["base_url"]

    test_path = configuration["test_path"]

    test_passed = _check_test_path(urljoin(base_url, test_path))

    if test_passed:
        _provision_source_type(
            name="web_page_content",
            description="The extracted content from a web page, converted into a markdown.",
            required_fields=["url"],
        )

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

    else:
        job.status = JobStatus.FAILED

        job.status_message = "Test path request failed ... unable to use the Web Site"

    job.ended = datetime.now(tz=utz_tz)

    jobs.put(job)