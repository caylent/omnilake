"""
Vector storage service event types
"""

from da_vinci.core.immutable_object import (
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)


class VectorArchiveGenerateEntryTagsEventBodySchema(ObjectBodySchema):
    """
    The body of the omnilake_basic_archive_generate_entry_tags event.
    """
    attributes = [
        SchemaAttribute(
            name='archive_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='content',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='entry_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='event_type',
            type=SchemaAttributeType.STRING,
            required=False,
            default_value='omnilake_archive_vector_generate_entry_tags',
        ),

        SchemaAttribute(
            name='parent_job_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='parent_job_type',
            type=SchemaAttributeType.STRING,
            required=True,
        ),
    ]


class VectorArchiveVacuumSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name="archive_id",
            type=SchemaAttributeType.STRING
        ),

        SchemaAttribute(
            name="entry_id",
            type=SchemaAttributeType.STRING
        ),

        SchemaAttribute(
            name="event_type",
            type=SchemaAttributeType.STRING,
            required=False,
            default_value="omnilake_archive_vector_vacuum_request",
        ),
    ]