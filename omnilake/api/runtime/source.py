'''
Handles the source API
'''
from datetime import datetime
from typing import Optional, Union

from da_vinci.event_bus.client import EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.api.runtime.construct import ChildAPI, Route

from omnilake.internal_lib.event_definitions import ReapSourceBody
from omnilake.internal_lib.job_types import JobType
from omnilake.internal_lib.naming import SourceResourceName

from omnilake.tables.jobs.client import Job, JobsClient
from omnilake.tables.sources.client import Source, SourcesClient


class SourcesAPI(ChildAPI):
    routes = [
        Route(
            path='/add_source',
            method_name='add_source',
        ),
        Route(
            path='/delete_source',
            method_name='delete_source',
        ),
        Route(
            path='/describe_source',
            method_name='describe_source',
        ),
    ]

    def add_source(self, source_category: str, location_id: str, metadata: Optional[dict] = None,
                   publication_date: Optional[Union[datetime, str]] = None):
        """
        Add a source, idempotent

        Keyword arguments:
        source_category -- The source category
        location_id -- The location ID
        metadata -- The metadata, optional
        publication_date -- The publication date, optional
        """
        sources = SourcesClient()

        existing_source = sources.get(
            source_category=source_category,
            location_id=location_id
        )

        if existing_source:
            existing_resource_name = SourceResourceName(resource_id=existing_source.source_id)

            return self.respond(
                body={
                    'source_id': existing_source.source_id,
                    'resource_name': str(existing_resource_name),
                },
                status_code=200,
            )

        source = Source(
            source_category=source_category,
            location_id=location_id,
            metadata=metadata,
            publication_date=publication_date,
        )

        sources.put(source)

        resource_name = SourceResourceName(resource_id=source.source_id)

        return self.respond(
            body={
                'source_id': source.source_id,
                'resource_name': str(resource_name),
            },
            status_code=201,
        )

    def delete_source(self, source_id: str, force: bool = False):
        """
        Delete a source

        Keyword arguments:
        source_id -- The source ID
        """
        sources = SourcesClient()

        source = sources.get(source_id=source_id)

        if not source:
            return self.respond(
                body='Source not found',
                status_code=404,
            )

        job = Job(job_type=JobType.DELETE_SOURCE)

        jobs = JobsClient()

        jobs.put(job)

        event_publisher = EventPublisher()

        event = EventBusEvent(
            body=ReapSourceBody(
                archive_id=source.source_id,
                source_id=source.source_id,
                force=force,
                job_id=job.job_id,
            ).to_dict(),
            event_type=ReapSourceBody.event_type,
        )

        event_publisher.submit(event)

        return self.respond(
            body=job.to_dict(json_compatible=True, exclude_attribute_names=['ai_statistics']),
            status_code=201,
        )

    def describe_source(self, source_id: str):
        """
        Describe a source

        Keyword arguments:
        source_id -- The source ID
        """
        sources = SourcesClient()

        source = sources.get(source_id=source_id)

        if not source:
            return self.respond(
                body='Source not found',
                status_code=404,
            )

        return self.respond(
            body=source.to_dict(json_compatible=True),
            status_code=200,
        )