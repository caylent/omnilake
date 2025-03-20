from da_vinci.core.immutable_object import (
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType
)

# Schema for indexing events
class IndexEventSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='configuration',
            type=SchemaAttributeType.OBJECT,
            required=True,
            description='Configuration details, must include the S3 bucket name.'
        ),
        SchemaAttribute(
            name='index_key',
            type=SchemaAttributeType.STRING,
            required=True,
            description='The S3 object key of the document to index.'
        ),
    ]

# Schema for lookup events
class LookupEventSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='configuration',
            type=SchemaAttributeType.OBJECT,
            required=True,
            description='Configuration details, must include the S3 bucket name.'
        ),
        SchemaAttribute(
            name='lookup_key',
            type=SchemaAttributeType.STRING,
            required=True,
            description='The S3 object key of the document to retrieve.'
        ),
    ]

# Schema for provisioning events
class ProvisionerEventSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='configuration',
            type=SchemaAttributeType.OBJECT,
            required=True,
            description='Configuration details, including bucket name and settings.'
        ),
    ]

# Schema for tag-generation events
class GenerateTagsEventSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='configuration',
            type=SchemaAttributeType.OBJECT,
            required=True,
            description='Configuration details, must include the S3 bucket name.'
        ),
        SchemaAttribute(
            name='index_key',
            type=SchemaAttributeType.STRING,
            required=True,
            description='The S3 object key of the document for tag generation.'
        ),
    ]
