"""
Contains the EntriesAPI class, which is a child API of the OmniLakeAPI class.
"""
from datetime import datetime

from da_vinci.core.immutable_object import (
    ObjectBody,
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)

from da_vinci.event_bus.client import EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.api.runtime.base import ChildAPI, Route

from omnilake.internal_lib.clients import RawStorageManager
from omnilake.internal_lib.event_definitions import (
    AddEntryEventBodySchema,
    IndexEntryEventBodySchema,
)
from omnilake.internal_lib.naming import OmniLakeResourceName

from omnilake.tables.entries.client import EntriesClient
from omnilake.tables.jobs.client import Job, JobsClient
from omnilake.tables.provisioned_archives.client import ArchivesClient
from omnilake.tables.registered_request_constructs.client import (
    RegisteredRequestConstructsClient,
    RequestConstructType,
)


class AddEntryRequestSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='destination_archive_id',
            type=SchemaAttributeType.STRING,
            required=False,
        ),

        SchemaAttribute(
            name='content',
            type=SchemaAttributeType.STRING,
        ),

        SchemaAttribute(
            name='effective_on',
            type=SchemaAttributeType.DATETIME,
            required=False,
        ),

        SchemaAttribute(
            name='original_of_source',
            type=SchemaAttributeType.STRING,
            required=False,
        ),

        SchemaAttribute(
            name='sources',
            type=SchemaAttributeType.STRING_LIST,
        ),
    ]


class DescribeEntryRequestSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='entry_id',
            type=SchemaAttributeType.STRING,
        ),
    ]


class GetEntryRequestSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='entry_id',
            type=SchemaAttributeType.STRING,
        ),
    ]


class EntriesAPI(ChildAPI):
    routes = [
        Route(
            path='/add_entry',
            method_name='add_entry',
            request_body_schema=AddEntryRequestSchema,
        ),
        Route(
            path='/describe_entry',
            method_name='describe_entry',
            request_body_schema=DescribeEntryRequestSchema,
        ),
        Route(
            path='/get_entry',
            method_name='get_entry',
            request_body_schema=GetEntryRequestSchema,
        ),
        Route(
            path='/index_entry',
            method_name='index_entry',
        ),
    ]

    def add_entry(self, request_body: ObjectBody):
        """
        Add an entry, idempotent

        Keyword arguments:
        request_body -- The request body
        """
        destination_archive_id = request_body.get("destination_archive_id")

        if destination_archive_id:
            archives = ArchivesClient()

            archive = archives.get(
                archive_id=destination_archive_id,
            )

            if not archive:
                return self.respond(
                    body={"message": "No such archive"},
                    status_code=400,
                )

        jobs = JobsClient()

        job = Job(job_type='ADD_ENTRY')

        jobs.put(job)

        sources = request_body["sources"]

        for source in sources:
            try:
                OmniLakeResourceName.from_string(source)

            except ValueError:
                return self.respond(
                    body={"message": "Invalid source detected, must be a valid OmniLake resource name"},
                    status_code=400,
                )

        effective_on = request_body.get("effective_on")

        if effective_on and isinstance(effective_on, datetime):
            effective_on = effective_on.isoformat()

        content = request_body["content"]

        original_of_source = request_body.get("original_of_source")

        event_body = ObjectBody(
            body={
                "destination_archive_id": destination_archive_id,
                "content": content,
                "effective_on": effective_on,
                "sources": sources,
                "job_id": job.job_id,
                "job_type": job.job_type,
                "original_of_source": original_of_source,
            },
            schema=AddEntryEventBodySchema,
        )

        event_publisher = EventPublisher()

        event = EventBusEvent(
            body=event_body.to_dict(ignore_unkown=True),
            event_type=event_body.get("event_type", strict=True),
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

        registered_constructs = RegisteredRequestConstructsClient()

        registered_construct = registered_constructs.get(
            registered_construct_type=RequestConstructType.ARCHIVE,
            registered_type_name=destination_archive.archive_type,
        )

        if not registered_construct:
            return self.respond(
                body={"message": "No registered construct for destination archive"},
                status_code=400,
            )

        if 'index' not in registered_construct.additional_supported_operations:
            return self.respond(
                body={"message": "Index operation not supported for destination archive"},
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

        job = Job(job_type='INDEX_ENTRY')

        jobs.put(job)

        event_body = ObjectBody(
            body={
                "archive_id": destination_archive_id,
                "effective_on": entry.effective_on,
                "entry_id": entry_id,
                "job_id": job.job_id,
                "job_type": job.job_type,
                "original_of_source": entry.original_of_source,
            },
            schema=IndexEntryEventBodySchema,
        )

        event_publisher = EventPublisher()

        event = EventBusEvent(
            event_type=registered_construct.get_operation_event_name('index'),
            body=event_body.to_dict(),
        )

        event_publisher.submit(event)

        return self.respond(
            body=job.to_dict(json_compatible=True),
            status_code=201,
        )

    def describe_entry(self, request_body: ObjectBody):
        """
        Describe an entry

        Keyword arguments:
        request_body -- The request body
        """
        entries = EntriesClient()

        entry_id = request_body.get("entry_id")

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

    def get_entry(self, request_body: ObjectBody):
        """
        Get an entry

        Keyword arguments:
        entry_id -- The entry ID
        """
        entries = EntriesClient()

        entry_id = request_body.get("entry_id")

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