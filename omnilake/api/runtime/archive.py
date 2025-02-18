"""
Archive API Declaration
"""
import logging

from da_vinci.core.immutable_object import (
    InvalidObjectSchemaError,
    ObjectBody,
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)

from da_vinci.event_bus.client import EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.api.runtime.base import ChildAPI, Route

from omnilake.internal_lib.event_definitions import ProvisionArchiveEventBodySchema
from omnilake.internal_lib.secrets import SSMSecretManager

from omnilake.tables.jobs.client import Job, JobsClient
from omnilake.tables.provisioned_archives.client import (
    Archive,
    ArchivesClient,
    ArchiveStatus,
)
from omnilake.tables.registered_request_constructs.client import (
    RegisteredRequestConstructsClient,
    RequestConstructType,
)


class CreateArchiveRequestSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='archive_id',
            type=SchemaAttributeType.STRING,
        ),

        SchemaAttribute(
            name='configuration',
            type=SchemaAttributeType.OBJECT,
        ),

        SchemaAttribute(
            name='description',
            type=SchemaAttributeType.STRING,
            required=False,
        ),

        SchemaAttribute(
            name='destination_archive_id',
            type=SchemaAttributeType.STRING,
            required=False,
        ),
    ]


class DescribeArchiveRequestSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='archive_id',
            type=SchemaAttributeType.STRING,
        ),
    ]


class ArchiveAPI(ChildAPI):
    routes = [
        Route(
            path='/create_archive',
            method_name='create_archive',
            request_body_schema=CreateArchiveRequestSchema,
        ),
        Route(
            path='/describe_archive',
            method_name='describe_archive',
            request_body_schema=DescribeArchiveRequestSchema,
        ),
        # TODO: Add support for deletion
    ]

    def create_archive(self, request: ObjectBody):
        """
        Create an archive

        Keyword arguments:
        request -- The request body
        """
        archives = ArchivesClient()

        archive_id = request.get("archive_id", strict=True)

        existing = archives.get(archive_id=archive_id)

        if existing:
            return self.respond(
                body={"message": "Archive already exists"},
                status_code=400,
            )

        configuration = request.get("configuration", strict=True)

        archive_type = configuration.get("archive_type")

        registered_constructs = RegisteredRequestConstructsClient()

        registered_construct = registered_constructs.get(
            registered_construct_type=RequestConstructType.ARCHIVE,
            registered_type_name=archive_type,
        )

        if not registered_construct:
            return self.respond(
                body={"message": "Invalid archive type"},
                status_code=400,
            )

        description = request.get("description")

        if registered_construct.schemas:
            provisioning_schema = registered_construct.get_object_body_schema(operation="provision")

            if provisioning_schema:
                logging.debug(f"Found provisioning schema: {provisioning_schema.to_dict()} ... validating configuration")

                try:
                    secret_mgr = SSMSecretManager()

                    # Initialize the object body, validating the configuration
                    config = ObjectBody(
                        body=configuration,
                        schema=provisioning_schema,
                        secret_masking_fn=secret_mgr.mask_secret,
                    )

                except InvalidObjectSchemaError as schema_error:
                    return self.respond(
                        body={"message": str(schema_error)},
                        status_code=400,
                    )

        archive_obj = Archive(
            archive_type=archive_type,
            archive_id=archive_id,
            configuration=config.to_dict(),
            description=description,
            status=ArchiveStatus.CREATING,
        )

        archives.put(archive_obj)

        job = Job(job_type='CREATE_ARCHIVE')

        jobs = JobsClient()

        jobs.put(job)

        event = EventBusEvent(
            body=ObjectBody(
                body={
                    "archive_id": archive_id,
                    "configuration": config.to_dict(),
                    "description": description,
                    "job_id": job.job_id,
                    "job_type": job.job_type
                },
                schema=ProvisionArchiveEventBodySchema,
            ).to_dict(),
            event_type=registered_construct.get_operation_event_name('provision'),
        )

        logging.debug(f"Submitting event: {event.to_dict()}")

        publisher = EventPublisher()

        publisher.submit(event)

        return self.respond(
            body=job.to_dict(json_compatible=True),
            status_code=201,
       )

    def describe_archive(self, request: ObjectBody):
        """
        Describe an archive

        Keyword arguments:
        archive_id -- The ID of the archive
        """
        archives = ArchivesClient()

        archive_id = request.get("archive_id")

        existing = archives.get(archive_id)

        if not existing:
            return self.respond(
                body={"message": "Archive does not exist"},
                status_code=404,
            )

        return self.respond(
            body=existing.to_dict(json_compatible=True),
            status_code=200,
        )