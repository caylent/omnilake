"""
Local event definitions for the knowledge graph processor.
"""

from da_vinci.core.immutable_object import (
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)


class FinalResponseRequestSchema(ObjectBodySchema):
    '''
    Event body to request the final response.
    '''
    attributes = [
        SchemaAttribute(
            name='entry_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='event_type',
            type=SchemaAttributeType.STRING,
            required=False,
            default_value='omnilake_processor_knowledge_graph_final_response_request',
        ),

        SchemaAttribute(
            name='goal',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='knowledge_graph_processing_id',
            type=SchemaAttributeType.STRING,
            required=True,
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
    ]


class KnowledgeAIFilteringCompleteSchema(ObjectBodySchema):
    '''
    Event body for AI filtering completion.
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
            default_value='omnilake_processor_knowledge_graph_ai_filtering_complete',
        ),

        SchemaAttribute(
            name='knowledge_graph_processing_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),
    ]


class KnowledgeAIFilteringRequestSchema(ObjectBodySchema):
    '''
    Event body for AI filtering request.
    '''
    attributes = [
        SchemaAttribute(
            name='goal',
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
            default_value='omnilake_processor_knowledge_graph_ai_filtering_request',
        ),

        SchemaAttribute(
            name='knowledge_graph_processing_id',
            type=SchemaAttributeType.STRING,
            required=True,
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
    ]


class KnowledgeExtractionCompleteSchema(ObjectBodySchema):
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
            default_value='omnilake_processor_knowledge_graph_extraction_complete',
        ),

        SchemaAttribute(
            name='knowledge_graph_processing_id',
            type=SchemaAttributeType.STRING,
            required=True,
        ),
    ]


class KnowledgeExtractionRequestSchema(ObjectBodySchema):
    '''
    Event body for summary request.
    '''
    attributes = [
        SchemaAttribute(
            name='entry_id',
            type=SchemaAttributeType.STRING_LIST,
            required=True,
        ),

        SchemaAttribute(
            name='event_type',
            type=SchemaAttributeType.STRING,
            required=False,
            default_value='omnilake_processor_knowledge_graph_extraction_request',
        ),

        SchemaAttribute(
            name='goal',
            type=SchemaAttributeType.STRING,
            required=False,
        ),

        SchemaAttribute(
            name='knowledge_graph_processing_id',
            type=SchemaAttributeType.STRING,
            required=True,
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
    ]