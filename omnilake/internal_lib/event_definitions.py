"""
Internal Event definitions for the Omnilake service.
"""
from da_vinci.core.immutable_object import (
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)

from omnilake.internal_lib.job_types import JobType


class AddEntryEventBodySchema(ObjectBodySchema):
    """
    The body of the omnilake_add_entry event.

    Attributes:
        archive_id (str): The ID of the archive to add the entry to.
        content (str): The content of the entry.
        effective_on (datetime): The effective date of the entry.
        event_type (str): The type of the event.
        job_id (str): The ID of the job.
        job_type (str): The type of the job.
        original_of_source (str): The original source of the entry.
        sources (List[str]): The sources of the entry.
        title (str): The title of the entry.
    """
    attributes = [
        SchemaAttribute(
            name='content',
            type=SchemaAttributeType.STRING,
            required=False,
        ),

        SchemaAttribute(
            name='destination_archive_id',
            type=SchemaAttributeType.STRING,
            required=False,
        ),

        SchemaAttribute(
            name='effective_on',
            type=SchemaAttributeType.DATETIME,
            required=False,
        ),

        SchemaAttribute(
            name='event_type',
            type=SchemaAttributeType.STRING,
            required=False,
            default_value='omnilake_add_entry',
        ),

        SchemaAttribute(
            name='job_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='job_type',
            type=SchemaAttributeType.STRING,
            required=False,
            default_value=JobType.ADD_ENTRY,
        ),

        SchemaAttribute(
            name='original_of_source',
            type=SchemaAttributeType.STRING,
            required=False,
        ),

        SchemaAttribute(
            name='sources',
            type=SchemaAttributeType.STRING_LIST,
            required=True,
        ),

        SchemaAttribute(
            name='title',
            type=SchemaAttributeType.STRING,
            required=False,
        ),
    ]


class LakeRequestInternalRequestEventBodySchema(ObjectBodySchema):
    attributes = [
        # The entry IDs to do something with, optional because Archives do not use them
        SchemaAttribute(
            name='entry_ids',
            type=SchemaAttributeType.STRING_LIST,
            required=False,
        ),

        SchemaAttribute(
            name='lake_request_id',
            type=SchemaAttributeType.STRING,
        ),

        SchemaAttribute(
            name='parent_job_id',
            type=SchemaAttributeType.STRING,
        ),

        SchemaAttribute(
            name='parent_job_type',
            type=SchemaAttributeType.STRING,
        ),

        SchemaAttribute(
            name='request_body',
            type=SchemaAttributeType.OBJECT,
        ),
    ]


class LakeRequestInternalResponseEventBodySchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='ai_invocation_ids',
            type=SchemaAttributeType.STRING_LIST,
            required=False,
        ),

        SchemaAttribute(
            name='entry_ids',
            type=SchemaAttributeType.STRING_LIST,
        ),

        SchemaAttribute(
            name='lake_request_id',
            type=SchemaAttributeType.STRING,
        ),

        SchemaAttribute(
            name='event_type',
            type=SchemaAttributeType.STRING,
            default_value='omnilake_lake_request_internal_stage_response',
            required=False,
        ),
    ]


class LakeRequestLookupResponse(ObjectBodySchema):
    '''
    Event schema for lookup response.
    '''
    attributes = [
        SchemaAttribute(
            name='lake_request_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),
        SchemaAttribute(
            name='entry_ids',
            type=SchemaAttributeType.STRING_LIST,
            required=True,
        ),
        SchemaAttribute(
            name="event_type",
            type=SchemaAttributeType.STRING,
            required=False,
            default_value="omnilake_lake_lookup_response",
        )
    ]


class IndexEntryEventBodySchema(ObjectBodySchema):
    """
    The body of the omnilake_index_entry event.

    No event_type attribute is defined here because it is dynamically managed through the
    registry.
    """
    attributes = [
        SchemaAttribute(
            name='archive_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='effective_on',
            type=SchemaAttributeType.DATETIME,
            required=False,
        ),

        SchemaAttribute(
            name='entry_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='original_of_source',
            type=SchemaAttributeType.STRING,
            required=False,
        ),

        SchemaAttribute(
            name='parent_job_id',
            type=SchemaAttributeType.STRING,
        ),

        SchemaAttribute(
            name='parent_job_type',
            type=SchemaAttributeType.STRING,
        ),
    ]


class LakeCompletionEventBodySchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='event_type',
            type=SchemaAttributeType.STRING,
            default_value='omnilake_lake_request_completion',
            required=False,
        ),

        SchemaAttribute(
            name='lake_request_id',
            type=SchemaAttributeType.STRING,
        ),

        SchemaAttribute(
            name='response_status',
            type=SchemaAttributeType.STRING,
        ),
    ]


class LakeChainRequestEventBodySchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='chain',
            type=SchemaAttributeType.OBJECT_LIST,
        ),

        SchemaAttribute(
            name='chain_request_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='job_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='job_type',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='event_type',
            type=SchemaAttributeType.STRING,
            default_value='omnilake_chain_request',
            required=False,
        )
    ]


class LakeRequestEventBodySchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='job_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='job_type',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='lake_request_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='lookup_instructions',
            type=SchemaAttributeType.OBJECT_LIST,
        ),

        SchemaAttribute(
            name='processing_instructions',
            type=SchemaAttributeType.OBJECT,
        ),

        SchemaAttribute(
            name='response_config',
            type=SchemaAttributeType.OBJECT,
            default_value={},
            required=False,
        ),

        SchemaAttribute(
            name='event_type',
            type=SchemaAttributeType.STRING,
            default_value='omnilake_lake_request',
            required=False,
        )
    ]


class ProvisionArchiveEventBodySchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='archive_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='configuration',
            type=SchemaAttributeType.OBJECT,
            required=False,
            default_value={},
        ),

        SchemaAttribute(
            name='description',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='job_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='job_type',
            type=SchemaAttributeType.STRING,
            required=False,
            default_value=JobType.CREATE_ARCHIVE,
        ),
    ]

