from da_vinci.core.immutable_object import (
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)


class RecursiveSummaryProcessor(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='include_source_metadata',
            type=SchemaAttributeType.BOOLEAN,
            required=False,
            default_value=False,
        ),

        SchemaAttribute(
            name='goal',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='model_id',
            type=SchemaAttributeType.STRING,
            required=False,
        ),

        SchemaAttribute(
            name='processor_type',
            type=SchemaAttributeType.STRING,
            default_value='SUMMARIZATION',
            required=False,
        ),

        SchemaAttribute(
            name='prompt',
            type=SchemaAttributeType.STRING,
            required=False,
        ),
    ]