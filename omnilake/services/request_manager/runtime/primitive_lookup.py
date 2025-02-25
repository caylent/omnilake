"""
Built-in primitive lookup functions
"""
import logging

from typing import List

from da_vinci.core.immutable_object import (
    ObjectBody,
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)
from da_vinci.core.logging import Logger

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from da_vinci.exception_trap.client import ExceptionReporter

from omnilake.internal_lib.event_definitions import (
    LakeRequestInternalRequestEventBodySchema,
    LakeRequestLookupResponse,
)

from omnilake.tables.entries.client import EntriesClient
from omnilake.tables.jobs.client import JobsClient
from omnilake.tables.lake_requests.client import LakeRequest, LakeRequestsClient
from omnilake.tables.sources.client import SourcesClient


class DirectEntryLookupSchema(ObjectBodySchema):
    attributes=[
        SchemaAttribute(
            name='entry_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='request_type',
            type=SchemaAttributeType.STRING,
            required=False,
            default_value='DIRECT_ENTRY',
        )
    ]


class DirectSourceLookupSchema(ObjectBodySchema):
    attributes=[
        SchemaAttribute(
            name='source_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='source_type',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='request_type',
            type=SchemaAttributeType.STRING,
            required=False,
            default_value='DIRECT_SOURCE',
        )
    ]


class RelatedRequestResponseLookupSchema(ObjectBodySchema):
    attributes=[
        SchemaAttribute(
            name='related_request_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='request_type',
            type=SchemaAttributeType.STRING,
            required=False,
            default_value='RELATED_RESPONSE',
        ),
    ]


class RelatedRequestSourcesLookupSchema(ObjectBodySchema):
    attributes=[
        SchemaAttribute(
            name='related_request_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='request_type',
            type=SchemaAttributeType.STRING,
            required=False,
            default_value='RELATED_SOURCES',
        ),
    ]


def _validate_entries(entry_ids: List[str]) -> None:
    '''
    Validates the entries

    Keyword arguments:
    resource_names -- The resource names
    '''
    entries = EntriesClient()

    for entry_id in entry_ids:
        entry = entries.get(entry_id=entry_id)

        if not entry:
            raise ValueError(f'Entry with ID {entry_id} does not exist')


def expand_source(source_id: str, source_type: str) -> str:
    '''
    Expands the sources

    Keyword arguments:
    source_id -- The source ID
    source_type -- The source type
    '''
    sources = SourcesClient()

    source = sources.get(source_type=source_type, source_id=source_id)

    if not source:
        raise ValueError(f'Source with ID {source_id} does not exist')

    if not source.latest_content_entry_id:
        raise ValueError(f'Source with ID {source_id} does not have any directly linked content')

    return source.latest_content_entry_id


def get_related_request(related_request_id: str) -> LakeRequest:
    '''
    Gets the related request

    Keyword arguments:
    related_request_id -- The related request ID
    '''
    lake_requests = LakeRequestsClient()

    related_request = lake_requests.get(lake_request_id=related_request_id)

    return related_request


_FN_NAME = 'omnilake.services.request_manager.primitive_lookup'


@fn_event_response(function_name=_FN_NAME, exception_reporter=ExceptionReporter(), logger=Logger(_FN_NAME))
def handler(event, context):
    """
    Handles a primitive lookup request
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=LakeRequestInternalRequestEventBodySchema,
    )

    jobs_client = JobsClient()

    parent_job = jobs_client.get(job_id=event_body.get('parent_job_id'), job_type=event_body.get('parent_job_type'))

    lookup_job = parent_job.create_child(job_type='OMNILAKE_PRIMITIVE_LOOKUP')

    jobs_client.put(job=parent_job)

    request_body = event_body.get('request_body')

    request_type = request_body['request_type']

    with jobs_client.job_execution(job=lookup_job, fail_parent=True):
        if request_type == 'DIRECT_ENTRY':
            entry_id = request_body['entry_id']

            logging.debug(f'Performing direct entry lookup for entry ID {entry_id}')

            _validate_entries(entry_ids=[entry_id])

            entry_ids = [entry_id]

        elif request_type == 'DIRECT_SOURCE':
            source_id = request_body['source_id']

            source_type = request_body['source_type']

            logging.debug(f'Performing direct source lookup for source ID {source_id} and source type {source_type}')

            entry_ids = [expand_source(source_id=source_id, source_type=source_type)]

            _validate_entries(entry_ids=[entry_id])

        elif request_type == 'RELATED_RESPONSE':
            request_id = event_body.get('request_id')

            logging.debug(f'Performing related response lookup for request ID {request_id}')

            related_request_id = request_body['related_request_id']

            related_request = get_related_request(related_request_id=related_request_id)

            if related_request:
                entry_ids = [related_request.response_entry_id]

            else:
                entry_ids = []

        elif request_type == 'RELATED_SOURCES':
            request_id = event_body.get('request_id')

            logging.debug(f'Performing related sources lookup for request ID {request_id}')

            related_request_id = request_body['related_request_id']

            related_request = get_related_request(related_request_id=related_request_id)

            if related_request:
                entry_ids = list(related_request.response_sources)

            else:
                entry_ids = []

        else:
            raise ValueError(f'Invalid request type {request_type}')

        response_obj = ObjectBody(
            body={
                "entry_ids": entry_ids,
                "lake_request_id": event_body['lake_request_id'],
            },
            schema=LakeRequestLookupResponse,
        )

        event_publisher = EventPublisher()

        event_publisher.submit(
            event=EventBusEvent(
                body=response_obj.to_dict(),
                event_type=response_obj.get("event_type", strict=True),
            ),
        )