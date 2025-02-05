from typing import List, Optional

from omnilake.client.client import (
    RequestAttributeType,
    RequestBodyAttribute,
    RequestBody,
)


## LakeArchiveConfigurations
class BasicArchiveConfiguration(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'archive_type',
            immutable_default='BASIC',
        ),

        RequestBodyAttribute(
            'retain_latest_originals_only',
            attribute_type=RequestAttributeType.BOOLEAN,
            default=True,
            optional=True,
        ),

        RequestBodyAttribute(
            'tag_hint_instructions',
            optional=True,
        ),

        RequestBodyAttribute(
            'tag_model_id',
            optional=True,
        ),
    ]


class VectorArchiveConfiguration(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'archive_type',
            immutable_default='VECTOR',
        ),

        RequestBodyAttribute(
            'chunk_body_overlap_percentage',
            attribute_type=RequestAttributeType.INTEGER,
            optional=True,
        ),

        RequestBodyAttribute(
            'max_chunk_length',
            attribute_type=RequestAttributeType.INTEGER,
            optional=True,
        ),

        RequestBodyAttribute(
            'retain_latest_originals_only',
            attribute_type=RequestAttributeType.BOOLEAN,
            default=True,
            optional=True,
        ),

        RequestBodyAttribute(
            'tag_hint_instructions',
            optional=True,
        ),

        RequestBodyAttribute(
            'tag_model_id',
            optional=True,
        ),
    ]

    def __init__(self, chunk_body_overlap_percentage: Optional[int] = None, max_chunk_length: Optional[int] = None,
                 retain_latest_originals_only: Optional[bool] = None, tag_hint_instructions: Optional[str] = None,
                 tag_model_id: Optional[str] = None):
        """
        Initialize the VectorArchiveConfiguration

        Keyword Arguments:
        chunk_body_overlap_percentage -- The chunk body overlap percentage for the vector archive, dictates how the vector
                                        ingestion process will chunk the body of the archive
        max_chunk_length -- The max chunk length for the vector archive, dictates the maximum length of a chunk
        retain_latest_originals_only -- Whether or not to retain only the latest originals
        tag_hint_instructions -- The tag hint instructions for the vector archive, dictates how the vector ingestion process
                                    will generate tags for the archive
        tag_model_id -- The tag model id for the vector archive, dictates the model to use for generating tags
        """
        super().__init__(
            chunk_body_overlap_percentage=chunk_body_overlap_percentage,
            max_chunk_length=max_chunk_length,
            retain_latest_originals_only=retain_latest_originals_only,
            tag_hint_instructions=tag_hint_instructions,
            tag_model_id=tag_model_id,
        )


## LakeRequestLookups
class BasicLookup(RequestBody):
    """
    Basic lookup instruction, it describes how the lake should lookup information.

    This is a basic lookup, it will lookup information based on the archive_id provided.

    Keyword Arguments:
    archive_id -- The archive_id to lookup
    max_entries -- The maximum number of entries to return
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'archive_id',
        ),

        RequestBodyAttribute(
            'lookup_type',
            immutable_default='BASIC',
        ),

        RequestBodyAttribute(
            'max_entries',
            attribute_type=RequestAttributeType.INTEGER,
        ),

        RequestBodyAttribute(
            'prioritize_tags',
            attribute_type=RequestAttributeType.LIST,
            optional=True,
        ),
    ]

    def __init__(self, archive_id: str, max_entries: int, prioritize_tags: Optional[List] = None):
        """
        Initialize the BasicLookup

        Keyword Arguments:
        archive_id -- The archive_id to lookup
        max_entries -- The maximum number of entries to return
        """
        super().__init__(
            archive_id=archive_id,
            max_entries=max_entries,
            prioritize_tags=prioritize_tags,
        )


class DirectEntryLookup(RequestBody):
    """
    This is a lookup instruction, it describes how the lake should lookup information.

    This is a direct lookup, it will lookup an entry based on the provided information.

    Keyword Arguments:
    resource_names -- The resource_names to lookup
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'request_type',
            immutable_default='DIRECT_ENTRY',
        ),

        RequestBodyAttribute(
            'entry_id',
            attribute_type=RequestAttributeType.STRING,
        ),
    ]

    def __init__(self, entry_id: str):
        """
        Initialize the DirectLookup

        Keyword Arguments:
        entry_id -- The entry_id to lookup
        """
        super().__init__(
            entry_id=entry_id,
        )


class DirectSourceLookup(RequestBody):
    """
    This is a lookup instruction, it describes how the lake should lookup information.

    This is a direct lookup, it will lookup a source based on the provided information.

    Keyword Arguments:
    resource_names -- The resource_names to lookup
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'request_type',
            immutable_default='DIRECT_SOURCE',
        ),

        RequestBodyAttribute(
            'source_id',
            attribute_type=RequestAttributeType.STRING,
        ),

        RequestBodyAttribute(
            'source_type',
            attribute_type=RequestAttributeType.STRING,
        ),
    ]

    def __init__(self, source_id: str, source_type: str):
        """
        Initialize the DirectLookup

        Keyword Arguments:
        source_id -- The source_id to lookup
        source_type -- The source_type to lookup
        """
        super().__init__(
            source_id=source_id,
            source_type=source_type,
        )


class RelatedRequestResponseLookup(RequestBody):
    """
    This is a lookup instruction, it describes how the lake should lookup information.

    This is a related request lookup, it will pull the response entry from the given related_request_id provided.
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'related_request_id',
        ),

        RequestBodyAttribute(
            'request_type',
            immutable_default='RELATED_RESPONSE',
        ),
    ]

    def __init__(self, related_request_id: str):
        """
        Initialize the RelatedRequestEntriesLookup

        Keyword Arguments:
        related_request_id -- The related_request_id to lookup
        """
        super().__init__(related_request_id=related_request_id)


class RelatedRequestSourcesLookup(RequestBody):
    """
    This is a lookup instruction, it describes how the lake should lookup information.

    This is a related request lookup, it will pull the sources used in a previous request.

    Keyword Arguments:
    related_request_id -- The related_request_id to lookup

    Example:
    ```
    lookup_instructions = RelatedRequestLookup(
        related_request_id='1234'
    )
    ```
    """
    attribute_definitions = [
        # One of related_request_id or related_request_name is required
        RequestBodyAttribute(
            'related_request_id',
        ),

        RequestBodyAttribute(
            'request_type',
            immutable_default='RELATED_SOURCES',
        ),
    ]

    def __init__(self, related_request_id: str = None, related_request_name: Optional[str] = None):
        """
        Initialize the RelatedRequestLookup

        Keyword Arguments:
        related_request_id -- The related_request_id to lookup
        related_request_name -- The related_request_name to lookup
        """
        if related_request_id is None and related_request_name is None:
            raise ValueError('One of related_request_id or related_request_name is required')

        super().__init__(
            related_request_id=related_request_id,
            related_request_name=related_request_name,
        )


class VectorLookup(RequestBody):
    """
    This is a lookup instruction, it describes how the lake should lookup information.

    This is a vector lookup, it will lookup information based on the archive_id and query string provided.

    Keyword Arguments:
    archive_id -- The archive_id to lookup
    max_entries -- The maximum number of entries to return
    query_string -- The query string the vector store will use for lookup
    prioritize_tags -- The tags to prioritize in the lookup
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'archive_id',
        ),

        RequestBodyAttribute(
            'max_entries',
            attribute_type=RequestAttributeType.INTEGER,
        ),

        RequestBodyAttribute(
            'query_string',
        ),

        RequestBodyAttribute(
            'prioritize_tags',
            attribute_type=RequestAttributeType.LIST,
            optional=True,
        ),

        RequestBodyAttribute(
            'request_type',
            immutable_default='VECTOR',
        )
    ]

    def __init__(self, archive_id: str, max_entries: int, query_string: str,
                 prioritize_tags: Optional[List] = None):
        """
        Initialize the VectorLookup

        Keyword Arguments:
        archive_id -- The archive_id to lookup
        max_entries -- The maximum number of entries to return
        query_string -- The query string to use for lookup
        prioritize_tags -- The tags to prioritize in the lookup
        """
        super().__init__(
            archive_id=archive_id,
            max_entries=max_entries,
            query_string=query_string,
            prioritize_tags=prioritize_tags,
        )


## LakeRequestProcessingInstructions
class KnowledgeGraphProcessor(RequestBody):
    """
    This is a Knowledge Graph processor, it extracts information from the provided entries and uses
    a modeled graph to generate a response.

    Keyword Arguments:

    Example:
    ```
    
    ```
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'ai_filter_include_goal',
            attribute_type=RequestAttributeType.BOOLEAN,
            default=False,
            optional=True,
        ),

        RequestBodyAttribute(
            'ai_filter_model_id',
            optional=True,
        ),

        RequestBodyAttribute(
            'community_filtering_max_group_size',
            attribute_type=RequestAttributeType.INTEGER,
            optional=True,
            default=150,
        ),

        RequestBodyAttribute(
            'community_filtering_threshold_min',
            attribute_type=RequestAttributeType.INTEGER,
            optional=True,
            default=50,
        ),

        RequestBodyAttribute(
            'goal',
            attribute_subtype=RequestAttributeType.STRING,
        ),

        RequestBodyAttribute(
            'knowledge_extraction_include_goal',
            attribute_type=RequestAttributeType.BOOLEAN,
            default=False,
            optional=True,
        ),

        RequestBodyAttribute(
            'knowledge_extraction_model_id',
            optional=True,
        ),

        # This is the minimum weight of a connection in the graph for it to be 
        # considered for the AI filtering and response generation
        RequestBodyAttribute(
            'minimally_considered_weight',
            attribute_type=RequestAttributeType.INTEGER,
            default=1,
            optional=True,
        ),

        RequestBodyAttribute(
            'processor_type',
            immutable_default='KNOWLEDGE_GRAPH',
        ),

        RequestBodyAttribute(
            'response_model_id',
            optional=True,
        ),

        # This is the % of communities that will be sent through AI filtering prior 
        # to response generation. This is always in descending order of community strength
        RequestBodyAttribute(
            'top_n_communities',
            attribute_type=RequestAttributeType.INTEGER,
            optional=True,
            default=80,
        ),
    ]

    def __init__(self, goal: str, ai_filter_include_goal: Optional[bool] = False, ai_filter_model_id: Optional[str] = None,
                 community_filtering_max_group_size: Optional[int] = None, community_filtering_threshold_min: Optional[int] = None,
                 knowledge_extraction_include_goal: Optional[bool] = False, knowledge_extraction_model_id: Optional[str] = None,
                 minimally_considered_weight: Optional[int] = None, response_model_id: Optional[str] = None,
                 top_n_communities: Optional[int] = None):
            """
            Initialize the KnowledgeGraphProcessor
    
            Keyword Arguments:
            goal -- The goal of the request
            ai_filter_include_goal -- Whether or not to include the goal in the AI filtering
            ai_filter_model_id -- The model_id to use for AI filtering
            community_filtering_max_group_size -- The maximum group size for community filtering
            community_filtering_threshold_min -- The minimum threshold for community filtering
            knowledge_extraction_include_goal -- Whether or not to include the goal in the knowledge extraction
            knowledge_extraction_model_id -- The model_id to use for knowledge extraction
            minimally_considered_weight -- The minimum weight of a connection in the graph for it to be considered for AI filtering and response generation
            response_model_id -- The model_id to use for response generation
            top_n_communities -- The % of communities that will be sent through AI filtering prior to response generation
            """
            super().__init__(
                goal=goal,
                ai_filter_include_goal=ai_filter_include_goal,
                ai_filter_model_id=ai_filter_model_id,
                community_filtering_max_group_size=community_filtering_max_group_size,
                community_filtering_threshold_min=community_filtering_threshold_min,
                knowledge_extraction_include_goal=knowledge_extraction_include_goal,
                knowledge_extraction_model_id=knowledge_extraction_model_id,
                minimally_considered_weight=minimally_considered_weight,
                response_model_id=response_model_id,
                top_n_communities=top_n_communities,
            )


class SummarizationProcessor(RequestBody):
    """
    This is a processing instruction, it describes how the lake should process the request.

    This is a summarization processor, it will summarize the information in the archive recursively.

    Keyword Arguments:
    - include_source_metadata: Whether or not to include the source metadata in the response
    - model_id: The model_id to use for summarization
    - prompt: The prompt to use for summarization

    Example:
    ```
    processing_instructions = SummarizationProcessor(
        include_source_metadata=True,
        model_id='anthropic.claude-3-5-sonnet-20241022-v2:0',
        prompt='Tell me about France'
    )
    ```
    """
    attribute_definitions = [
        # Must always have a goal, but it won't be used when the prompt is provided
        RequestBodyAttribute(
            'goal',
            attribute_subtype=RequestAttributeType.STRING,
        ),

        RequestBodyAttribute(
            'include_source_metadata',
            attribute_type=RequestAttributeType.BOOLEAN,
            optional=True,
        ),

        RequestBodyAttribute(
            'model_id',
            optional=True,
        ),

        RequestBodyAttribute(
            'processor_type',
            immutable_default='SUMMARIZATION',
        ),

        RequestBodyAttribute(
            'prompt',
            optional=True,
        ),
    ]

    def __init__(self, goal: str, include_source_metadata: Optional[bool] = None, model_id: Optional[str] = None,
                 prompt: Optional[str] = None):
        """
        Initialize the SummarizationProcessor

        Keyword Arguments:
        goal -- The goal of the request
        include_source_metadata -- Whether or not to include the source metadata in the response
        model_id -- The model_id to use for summarization
        prompt -- The prompt to use for summarization
        """
        super().__init__(
            goal=goal,
            include_source_metadata=include_source_metadata,
            model_id=model_id,
            prompt=prompt,
        )


## Response Configurations
class DirectResponseConfig(RequestBody):
    """
    Pass through responses from the processor directly. Useful if there is no need to
    further modify the response.
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'destination_archive_id',
            optional=True,
        ),

        RequestBodyAttribute(
            'response_type',
            immutable_default='DIRECT',
        )
    ]

    def __init__(self, destination_archive_id: Optional[str] = None):
        """
        Initialize the DirectResponseConfig

        Keyword Arguments:
        destination_archive_id -- The optional archive_id to store the response in
        """
        super().__init__(
            destination_archive_id=destination_archive_id,
        )


class SimpleResponseConfig(RequestBody):
    """
    This is a response configuration, it describes how the lake should craft it's response.

    This is a simple response configuration, it will return the most relevant information to the goal. If the prompt
    is provided, this will override the system defaults for the prompt.

    If the model_id is provided, this will override the system defaults for the model_id.

    Keyword Arguments:
    - goal: The goal of the request
    - model_id: The model_id to use for summarization
    - prompt: The prompt to use for summarization
    - destination_archive_id: The optional archive_id to store the response in

    Example:
    ```
    response_config = SimpleResponseConfig(
        goal='I want to know more about France',
        model_id='anthropic.claude-3-5-sonnet-20241022-v2:0',
        destination_archive_id='france_facts'
    )
    ```
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'destination_archive_id',
            optional=True,
        ),

        RequestBodyAttribute(
            'goal',
        ),

        RequestBodyAttribute(
            'model_id',
            optional=True,
        ),

        RequestBodyAttribute(
            'response_type',
            immutable_default='SIMPLE',
        )
    ]

    def __init__(self, goal: str, model_id: Optional[str] = None, prompt: Optional[str] = None,
                 destination_archive_id: Optional[str] = None):
        """
        Initialize the SimpleResponseConfig

        Keyword Arguments:
        goal -- The goal of the request
        model_id -- The model_id to use for summarization
        prompt -- The prompt to use for summarization
        destination_archive_id -- The optional archive_id to store the response in
        """
        super().__init__(
            goal=goal,
            model_id=model_id,
            prompt=prompt,
            destination_archive_id=destination_archive_id,
        )