"""
Handles the source API
"""
import re

from da_vinci.core.immutable_object import (
    ObjectBody,
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)

from omnilake.api.runtime.base import ChildAPI, Route

from omnilake.internal_lib.naming import SourceResourceName

from omnilake.tables.sources.client import Source, SourcesClient
from omnilake.tables.source_types.client import SourceType, SourceTypesClient


class AddSourceRequestSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='source_type',
            type=SchemaAttributeType.STRING,
        ),

        SchemaAttribute(
            name='source_arguments',
            type=SchemaAttributeType.OBJECT,
        ),
    ]


class CreateSourceTypeRequestSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='name',
            type=SchemaAttributeType.STRING,
        ),

        SchemaAttribute(
            name='required_fields',
            type=SchemaAttributeType.STRING_LIST,
        ),

        SchemaAttribute(
            name='description',
            type=SchemaAttributeType.STRING,
        ),
    ]


class DescribeSourceRequestSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='source_id',
            type=SchemaAttributeType.STRING,
        ),

        SchemaAttribute(
            name='source_type',
            type=SchemaAttributeType.STRING,
        ),
    ]


class DescribeSourceTypeRequestSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='source_type',
            type=SchemaAttributeType.STRING,
        ),
    ]


class SourcesAPI(ChildAPI):
    routes = [
        Route(
            path='/add_source',
            method_name='add_source',
            request_body_schema=AddSourceRequestSchema,
        ),
        Route(
            path='/create_source_type',
            method_name='create_source_type',
            request_body_schema=CreateSourceTypeRequestSchema,
        ),
        Route(
            path='/describe_source',
            method_name='describe_source',
            request_body_schema=DescribeSourceRequestSchema,
        ),
        Route(
            path='/describe_source_type',
            method_name='describe_source_type',
            request_body_schema=DescribeSourceTypeRequestSchema,
        )
    ]

    def add_source(self, request_body: ObjectBody):
        """
        Add a source, idempotent

        Keyword arguments:
        source_type -- The source type name
        source_arguments -- The source arguments
        """
        source_types = SourceTypesClient()

        source_type = request_body["source_type"]

        source_type_obj = source_types.get(source_type_name=source_type)

        if not source_type_obj:
            return self.respond(
                body={'message': 'Source type not found'},
                status_code=404,
            )

        sources = SourcesClient()

        source_arguments = request_body["source_arguments"]

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

    def create_source_type(self, request_body: ObjectBody):
        """
        Create a source type

        Keyword arguments:
        request_body -- The request body
        """
        source_types = SourceTypesClient()

        name = request_body["name"]

        if not re.match(r'^\w+$', name):
            return self.respond(
                body={'message': 'Invalid source type name'},
                status_code=400,
            )

        existing = source_types.get(source_type_name=name)

        if existing:
            return self.respond(
                body='Source type already exists',
                status_code=400,
            )

        description = request_body.get("description")

        required_fields = request_body.get("required_fields")

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

    def describe_source(self, request_body: ObjectBody):
        """
        Describe a source

        Keyword arguments:
        source_id -- The source ID
        source_type -- The source type
        """
        sources = SourcesClient()

        source_id = request_body.get("source_id")

        source_type = request_body.get("source_type")

        source = sources.get(source_type=source_type, source_id=source_id)

        if not source:
            return self.respond(
                body={'message': 'Source not found'},
                status_code=404,
            )

        return self.respond(
            body=source.to_dict(json_compatible=True),
            status_code=200,
        )

    def describe_source_type(self, request_body: ObjectBody):
        """
        Describe a source type

        Keyword arguments:
        source_type
        """
        source_types = SourceTypesClient()

        name = request_body.get("source_type")

        source_type_obj = source_types.get(source_type_name=name)

        if not source_type_obj:
            return self.respond(
                body={'message': 'Source type not found'},
                status_code=404,
            )

        return self.respond(
            body=source_type_obj.to_dict(json_compatible=True),
            status_code=200,
        )