from da_vinci.core.immutable_object import (
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)


class InceptionProcessorSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='chain_definition',
            type=SchemaAttributeType.OBJECT_LIST,
            required=True,
        ),

        SchemaAttribute(
            name='entry_distribution_mode',
            type=SchemaAttributeType.STRING,
            required=False,
            default_value='ALL',
        ),

        # This is any valid OmniLake processor construct
        SchemaAttribute(
            name='join_instructions',
            type=SchemaAttributeType.OBJECT,
            required=False,
        ),
    ]