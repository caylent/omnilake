from da_vinci.core.immutable_object import (
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)


class KnowledgeGraphProcessorSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='ai_filter_include_goal',
            type=SchemaAttributeType.BOOLEAN,
            default_value=False,
            required=False,
        ),

        SchemaAttribute(
            name='ai_filter_model_id',
            type=SchemaAttributeType.STRING,
            required=False,
        ),

        SchemaAttribute(
            name='community_filtering_threshold_min',
            type=SchemaAttributeType.NUMBER,
            default_value=50,
            required=False,
        ),

        SchemaAttribute(
            name='community_filtering_max_group_size',
            type=SchemaAttributeType.NUMBER,
            default_value=150,
            required=False,
        ),

        SchemaAttribute(
            name='goal',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='knowledge_extraction_include_goal',
            type=SchemaAttributeType.BOOLEAN,
            default_value=False,
            required=False,
        ),

        SchemaAttribute(
            name='knowledge_extraction_model_id',
            type=SchemaAttributeType.STRING,
            required=False,
        ),

        SchemaAttribute(
            name='minimally_considered_weight',
            type=SchemaAttributeType.NUMBER,
            default_value=1,
            required=False,
        ),

        SchemaAttribute(
            name='processor_type',
            type=SchemaAttributeType.STRING,
            default_value='KNOWLEDGE_GRAPH',
            required=False,
        ),

        SchemaAttribute(
            name='response_model_id',
            type=SchemaAttributeType.STRING,
            required=False,
        ),

        SchemaAttribute(
            name='top_n_communities',
            type=SchemaAttributeType.NUMBER,
            default_value=80,
            required=False,
        ),
    ]