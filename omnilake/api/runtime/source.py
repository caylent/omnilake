'''
Handles the source API
'''
from typing import Dict, List, Optional

from da_vinci.event_bus.client import EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.api.runtime.construct import ChildAPI, Route

from omnilake.internal_lib.event_definitions import ReapSourceBody
from omnilake.internal_lib.job_types import JobType
from omnilake.internal_lib.naming import SourceResourceName

from omnilake.tables.jobs.client import Job, JobsClient
from omnilake.tables.sources.client import Source, SourcesClient
from omnilake.tables.source_types.client import SourceType, SourceTypesClient


class SourcesAPI(ChildAPI):
    routes = [
        Route(
            path='/add_source',
            method_name='add_source',
        ),
        Route(
            path='/create_source_type',
            method_name='create_source_type',
        ),
        Route(
            path='/delete_source',
            method_name='delete_source',
        ),
        Route(
            path='/describe_source',
            method_name='describe_source',
        ),
        Route(
            path='/describe_source_type',
            method_name='describe_source_type',
        )
    ]

    def add_source(self, source_type: str, source_arguments: Dict):
        """
        Add a source, idempotent

        Keyword arguments:
        source_type -- The source type name
        source_arguments -- The source arguments
        """
        source_types = SourceTypesClient()

        source_type_obj = source_types.get(source_type_name=source_type)

        if not source_type_obj:
            return self.respond(
                body='Source type not found',
                status_code=404,
            )

        sources = SourcesClient()

        try:
            attribute_key = source_type_obj.generate_key(source_arguments=source_arguments)

        except ValueError as e:
            return self.respond(
                body=str(e),
                status_code=400,
            )

        existing_source = sources.get_by_attribute_key(attribute_key=attribute_key)

        if existing_source:
            existing_resource_name = SourceResourceName(
                resource_id=existing_source.source_type + '/' + existing_source.source_id
            )

            return self.respond(
                body={
                    'source_id': existing_source.source_id,
                    'resource_name': str(existing_resource_name),
                },
                status_code=200,
            )

        source = Source(
            source_type=source_type,
            attribute_key=attribute_key,
            source_arguments=source_arguments
        )

        sources.put(source)

        resource_name = SourceResourceName(resource_id=source.source_type + '/' + source.source_id)

        return self.respond(
            body={
                'source_id': source.source_id,
                'resource_name': str(resource_name),
            },
            status_code=201,
        )

    def create_source_type(self, name: str, required_fields: List[str], description: Optional[str] = None):
        """
        Create a source type

        Keyword arguments:
        name -- The source type name
        required_fields -- The required field names
        description -- The description, optional
        """
        source_types = SourceTypesClient()

        existing = source_types.get(source_type_name=name)

        if existing:
            return self.respond(
                body='Source type already exists',
                status_code=400,
            )

        source_type = SourceType(
            source_type_name=name,
            required_fields=required_fields,
            description=description,
        )

        source_types.put(source_type)

        return self.respond(
            body=source_type.to_dict(json_compatible=True),
            status_code=201,
        )

    def delete_source(self, source_type: str, source_id: str, force: bool = False):
        """
        Delete a source

        Keyword arguments:
        source_id -- The source ID
        """
        sources = SourcesClient()

        source = sources.get(source_type=source_type, source_id=source_id)

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
                source_type=source.source_type,
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

    def describe_source(self, source_type: str, source_id: str):
        """
        Describe a source

        Keyword arguments:
        source_id -- The source ID
        source_type -- The source type
        """
        sources = SourcesClient()

        source = sources.get(source_type=source_type, source_id=source_id)

        if not source:
            return self.respond(
                body='Source not found',
                status_code=404,
            )

        return self.respond(
            body=source.to_dict(json_compatible=True),
            status_code=200,
        )

    def describe_source_type(self, name: str):
        """
        Describe a source type

        Keyword arguments:
        source_type
        """
        source_types = SourceTypesClient()

        source_type_obj = source_types.get(source_type_name=name)

        if not source_type_obj:
            return self.respond(
                body='Source type not found',
                status_code=404,
            )

        return self.respond(
            body=source_type_obj.to_dict(json_compatible=True),
            status_code=200,
        )