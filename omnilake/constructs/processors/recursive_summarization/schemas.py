from da_vinci.core.immutable_object import (
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)


class RecursiveSummaryProcessor(ObjectBodySchema):
    attributes = [
        # How to determine the effective_on date for the final response
        # Support for RUNTIME, NEWEST, OLDEST, and AVERAGE
        SchemaAttribute(
            name='effective_on_calculation_rule',
            type=SchemaAttributeType.STRING,
            required=False,
            default_value='RUNTIME',
        ),

        SchemaAttribute(
            name='goal',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='include_source_metadata',
            type=SchemaAttributeType.BOOLEAN,
            required=False,
            default_value=False,
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