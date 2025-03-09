"""
Local event definitions for the recursive summarization processor.
"""

from da_vinci.core.immutable_object import (
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)


class SummarizationCompletedSchema(ObjectBodySchema):
    '''
    Event body for summary completion.
    '''
    attributes = [
        SchemaAttribute(
            name='ai_invocation_id',
            type=SchemaAttributeType.STRING,
            required=False,
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
            default_value='omnilake_processor_summarizer_summary_complete',
        ),

        SchemaAttribute(
            name='summary_request_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),
    ]


class SummarizationRequestSchema(ObjectBodySchema):
    '''
    Event body for summary request.
    '''
    attributes = [
        SchemaAttribute(
            name='effective_on_calculation_rule',
            type=SchemaAttributeType.STRING,
            required=False,
            default_value='RUNTIME',
        ),

        SchemaAttribute(
            name='entry_ids',
            type=SchemaAttributeType.STRING_LIST,
            required=True,
        ),

        SchemaAttribute(
            name='event_type',
            type=SchemaAttributeType.STRING,
            required=False,
            default_value='omnilake_processor_summarizer_summary_request',
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
            name='parent_job_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='parent_job_type',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='prompt',
            type=SchemaAttributeType.STRING,
            required=False,
        ),

        SchemaAttribute(
            name='summary_request_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),
    ]