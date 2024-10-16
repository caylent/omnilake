'''
Archive API Declaration
'''
from typing import Optional

from da_vinci.event_bus.client import EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.api.runtime.construct import ChildAPI, Route

from omnilake.internal_lib.event_definitions import CreateArchiveBody
from omnilake.internal_lib.job_types import JobType

from omnilake.tables.archives.client import ArchivesClient
from omnilake.tables.jobs.client import Job, JobsClient


class ArchiveAPI(ChildAPI):
    routes = [
        Route(
            path='/create_archive',
            method_name='create_archive',
        )
    ]

    def create_archive(self, archive_id: str, description: str, storage_type: Optional[str] = 'VECTOR_STORAGE'):
        """
        Create an archive

        Keyword arguments:
        archive_id -- The ID of the archive
        description -- The description of the archive
        storage_type -- The storage type of the archive
        """
        archives = ArchivesClient()

        existing = archives.get(archive_id)

        if existing:
            return self.respond(
                body={"message": "Archive already exists"},
                status_code=400,
            )

        job = Job(job_type=JobType.CREATE_ARCHIVE)

        jobs = JobsClient()

        jobs.put(job)

        event = EventBusEvent(
            event_type=CreateArchiveBody.event_type,
            body=CreateArchiveBody(
                archive_id=archive_id,
                description=description,
                job_id=job.job_id,
                storage_type=storage_type,
            ).to_dict(),
        )

        publisher = EventPublisher()

        publisher.submit(event)

        return self.respond(
            body=job.to_dict(json_compatible=True),
            status_code=200,
       )