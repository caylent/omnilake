from da_vinci.core.immutable_object import (
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)


class WebSiteArchiveLookupObjectSchema(ObjectBodySchema):
    """Basic Archive Lookup Object Schema"""
    attributes=[
        SchemaAttribute(
            name='archive_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),
        SchemaAttribute(
            name='retrieve_paths',
            type=SchemaAttributeType.STRING_LIST,
            required=True,
        ),
    ]


class WebSiteArchiveProvisionObjectSchema(ObjectBodySchema):
    """Basic Archive Provision Object Schema"""
    attributes=[
        SchemaAttribute(
            name='base_url',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='test_path',
            type=SchemaAttributeType.STRING,
            required=True,
        ),
    ]