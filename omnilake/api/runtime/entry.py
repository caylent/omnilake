'''
Contains the EntriesAPI class, which is a child API of the OmniLakeAPI class.
'''
from datetime import datetime
from typing import List, Union

from da_vinci.event_bus.client import EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.api.runtime.construct import ChildAPI, Route

from omnilake.internal_lib.event_definitions import (
    AddEntryBody,
    IndexEntryBody,
    ReapEntryBody,
    UpdateEntryBody,
)
from omnilake.internal_lib.clients import RawStorageManager
from omnilake.internal_lib.job_types import JobType
from omnilake.internal_lib.naming import OmniLakeResourceName

from omnilake.tables.archives.client import ArchivesClient
from omnilake.tables.entries.client import EntriesClient
from omnilake.tables.jobs.client import Job, JobsClient


class EntriesAPI(ChildAPI):
    routes = [
        Route(
            path='/add_entry',
            method_name='add_entry',
        ),
        Route(
            path='/delete_entry',
            method_name='delete_entry',
        ),
        Route(
            path='/describe_entry',
            method_name='describe_entry',
        ),
        Route(
            path='/get_entry',
            method_name='get_entry',
        ),
        Route(
            path='/index_entry',
            method_name='index_entry',
        ),
        Route(
            path='/update_entry',
            method_name='update_entry',
        ),
    ]

    def add_entry(self, archive_id: str, content: str, sources: List[str], effective_on: Union[datetime, str] = None,
                  original: str = None, summarize: bool = False):
        """
        Add an entry, idempotent

        Keyword arguments:
        archive_id -- The archive ID
        content -- The content
        sources -- The resource names of the sources
        effective_on -- The effective date and time
        original -- The original entry, optional
        summarize -- Whether to summarize the entry, optional
        """
        archives = ArchivesClient()

        archive = archives.get(
            archive_id=archive_id,
        )

        if not archive:
            return self.respond(
                body={"message": "No such archive"},
                status_code=400,
            )

        jobs = JobsClient()

        job = Job(job_type=JobType.ADD_ENTRY)

        jobs.put(job)

        for source in sources:
            try:
                OmniLakeResourceName.from_string(source)

            except ValueError:
                return self.respond(
                    body={"message": "Invalid source detected, must be a valid OmniLake resource name"},
                    status_code=400,
                )

        if effective_on and isinstance(effective_on, datetime):
            effective_on = effective_on.isoformat()

        event_body = AddEntryBody(
            archive_id=archive_id,
            content=content,
            effective_on=effective_on,
            sources=sources,
            job_id=job.job_id,
            original=original,
            summarize=summarize,
        )

        event_publisher = EventPublisher()

        event = EventBusEvent(
            event_type=event_body.event_type,
            body=event_body.to_dict(),
        )

        event_publisher.submit(event)

        return self.respond(
            body=job.to_dict(json_compatible=True),
            status_code=201,
        )

    def index_entry(self, destination_archive_id: str, entry_id: str):
        """
        Index an entry into a destination archive, idempotent

        Keyword arguments:
        destination_archive_id -- The destination archive ID
        source_archive_id -- The source archive ID
        entry_id -- The entry ID
        """
        archives = ArchivesClient()

        destination_archive = archives.get(
            archive_id=destination_archive_id,
        )

        if not destination_archive:
            return self.respond(
                body={"message": "Invalid destination archive"},
                status_code=400,
            )

        entries = EntriesClient()

        entry = entries.get(
            entry_id=entry_id,
        )

        if not entry:
            return self.respond(
                body={"message": "Entry not found"},
                status_code=404,
            )

        if destination_archive_id in entry.archives:
            return self.respond(
                body={"message": "Entry already exists in destination archive"},
                status_code=400,
            )

        jobs = JobsClient()

        job = Job(job_type=JobType.INDEX_ENTRY)

        jobs.put(job)

        event_body = IndexEntryBody(
            archive_id=destination_archive_id,
            entry_id=entry_id,
            job_id=job.job_id,
        )

        event_publisher = EventPublisher()

        event = EventBusEvent(
            event_type=event_body.event_type,
            body=event_body.to_dict(),
        )

        event_publisher.submit(event)

        return self.respond(
            body=job.to_dict(json_compatible=True),
            status_code=201,
        )

    def delete_entry(self, archive_id: str, entry_id: str):
        """
        Delete an entry, idempotent

        Keyword arguments:
        archive_id -- The archive ID
        entry_id -- The entry ID
        """
        jobs = JobsClient()

        job = Job(job_type=JobType.DELETE_ENTRY)

        jobs.put(job)

        event_body = ReapEntryBody(
            archive_id=archive_id,
            entry_id=entry_id,
            job_id=job.job_id,
        )

        event_publisher = EventPublisher()

        event = EventBusEvent(
            event_type=event_body.event_type,
            body=event_body.to_dict(),
        )

        event_publisher.submit(event)

        return self.respond(
            body={'job_id': job.job_id},
            status_code=202,
        )

    def describe_entry(self, entry_id: str):
        """
        Describe an entry

        Keyword arguments:
        archive_id -- The archive ID
        entry_id -- The entry ID
        """
        entries = EntriesClient()

        entry = entries.get(
            entry_id=entry_id,
        )

        if not entry:
            return self.respond(
                body={"message": "Entry not found"},
                status_code=404,
            )

        entry_dict = entry.to_dict(json_compatible=True)

        resource_name = OmniLakeResourceName()('entry', entry_id)

        entry_dict['resource_name'] = str(resource_name)

        return self.respond(
            body=entry.to_dict(json_compatible=True),
            status_code=200,
       )

    def get_entry(self, entry_id: str):
        """
        Get an entry

        Keyword arguments:
        entry_id -- The entry ID
        """
        entries = EntriesClient()

        entry = entries.get(
            entry_id=entry_id,
        )

        if not entry:
            return self.respond(
                body={"message": "Entry not found"},
                status_code=404,
            )

        raw_mgr = RawStorageManager()

        try:
            response = raw_mgr.get_entry(entry_id)

            content = response.response_body['content']

        except Exception as e:
            return self.respond(
                body={"message": "Error fetching entry content"},
                status_code=500,
            )

        return self.respond(
            body={"content": content},
            status_code=200,
        )

    def update_entry(self, entry_id: str, content: str):
        """
        Update an entry, idempotent

        Keyword arguments:
        entry_id -- The entry ID
        content -- The content
        """
        jobs = JobsClient()

        job = Job(job_type=JobType.UPDATE_ENTRY)

        jobs.put(job)

        event_body = UpdateEntryBody(
            job_id=job.job_id,
            entry_id=entry_id,
            content=content,
        )

        event_publisher = EventPublisher()

        event = EventBusEvent(
            event_type=event_body.event_type,
            body=event_body.to_dict(),
        )

        event_publisher.submit(event)

        return self.respond(
            body=job.to_dict(json_compatible=True),
            status_code=202,
        )