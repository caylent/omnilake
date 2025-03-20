from da_vinci.core.immutable_object import ObjectBodySchema, SchemaAttribute, SchemaAttributeType

# Schema for provisioning the archive (e.g., creating/checking S3 bucket)
class ProvisioningSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='bucket',
            type=SchemaAttributeType.STRING,
            required=True,
            description='Name of the S3 bucket to store and retrieve documents.'
        ),
    ]

# Schema for configuration used across indexing and lookups
class ArchiveConfigurationSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='bucket',
            type=SchemaAttributeType.STRING,
            required=True,
            description='Name of the S3 bucket associated with this archive.'
        ),
        SchemaAttribute(
            name='prefix',
            type=SchemaAttributeType.STRING,
            required=False,
            description='Optional prefix/path inside the bucket to scope operations.'
        ),
    ]

# Schema for indexing a document from S3
class IndexRequestSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='index_key',
            type=SchemaAttributeType.STRING,
            required=True,
            description='The S3 object key of the document to be indexed.'
        ),
    ]

# Schema for lookup requests to retrieve documents
class LookupRequestSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='lookup_key',
            type=SchemaAttributeType.STRING,
            required=True,
            description='The S3 object key of the document to retrieve.'
        ),
    ]

# Schema for the response after performing a lookup
class LookupResponseSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='key',
            type=SchemaAttributeType.STRING,
            required=True,
            description='The S3 object key of the retrieved document.'
        ),
        SchemaAttribute(
            name='content',
            type=SchemaAttributeType.STRING,
            required=True,
            description='The textual content of the retrieved document.'
        ),
    ]
