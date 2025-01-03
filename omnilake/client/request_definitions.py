from omnilake.client.client import (
    RequestAttributeType,
    RequestBodyAttribute,
    RequestBody,
)


class AddEntry(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'content',
        ),

        RequestBodyAttribute(
            'sources',
            attribute_type=RequestAttributeType.LIST,
        ),

        RequestBodyAttribute(
            'archive_id',
            optional=True,
        ),

        RequestBodyAttribute(
            'effective_on',
            attribute_type=RequestAttributeType.DATETIME,
            optional=True,
        ),

        RequestBodyAttribute(
            'original_of_source',
            optional=True,
        ),

        RequestBodyAttribute(
            'summarize',
            attribute_type=RequestAttributeType.BOOLEAN,
            optional=True,
        )
    ]

    path = '/add_entry'


class AddSource(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'source_type',
        ),

        RequestBodyAttribute(
            'source_arguments',
            attribute_type=RequestAttributeType.OBJECT,
        )
    ]

    path = '/add_source'


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
    ]


class VectorArchiveConfiguration(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'archive_type',
            immutable_default='VECTOR',
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
    ]


class CreateArchive(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'archive_id',
        ),

        RequestBodyAttribute(
            'configuration',
            attribute_type=RequestAttributeType.OBJECT,
            supported_request_body_types=[BasicArchiveConfiguration, VectorArchiveConfiguration],
        ),

        RequestBodyAttribute(
            'description',
            optional=True,
        ),
    ]

    path = '/create_archive'


class CreateSourceType(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'name',
        ),

        RequestBodyAttribute(
            'required_fields',
            attribute_type=RequestAttributeType.LIST,
        ),

        RequestBodyAttribute(
            'description',
            optional=True,
        )
    ]

    path = '/create_source_type'


class DeleteEntry(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'entry_id',
        ),
    ]

    path = '/delete_entry'


class DeleteSource(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'source_id',
        ),

        RequestBodyAttribute(
            'source_type',
        )
    ]

    path = '/delete_source'


class DescribeArchive(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'archive_id',
        )
    ]

    path = '/describe_archive'


class DescribeEntry(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'entry_id',
        )
    ]

    path = '/describe_entry'


class DescribeJob(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'job_id',
        ),

        RequestBodyAttribute(
            'job_type',
        )
    ]

    path = '/describe_job'


class DescribeSource(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'source_id',
        ),

        RequestBodyAttribute(
            'source_type',
        )
    ]

    path = '/describe_source'


class DescribeSourceType(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'name',
        )
    ]

    path = '/describe_source_type'


class DescribeLakeRequest(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'request_id',
        )
    ]

    path = '/describe_lake_request'


class GetEntry(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'entry_id',
        )
    ]

    path = '/get_entry'


class IndexEntry(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'archive_id',
        ),

        RequestBodyAttribute(
            'entry_id',
        )
    ]

    path = '/index_entry'


class BasicLookup(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'archive_id',
        ),

        RequestBodyAttribute(
            'max_entries',
            attribute_type=RequestAttributeType.INTEGER,
        ),

        RequestBodyAttribute(
            'request_type',
            immutable_default='BASIC',
        )
    ]


class DirectLookup(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'resource_names',
            attribute_type=RequestAttributeType.LIST,
        )
    ]


class RelatedRequestLookup(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'related_request_id',
        ),

        RequestBodyAttribute(
            'request_type',
            immutable_default='RELATED',
        )
    ]


class VectorLookup(RequestBody):
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
            optional=True,
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


class SummarizationAlgorithm(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'algorithm',
            immutable_default='SUMMARIZATION',
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
            'prompt',
            optional=True,
        ),
    ]


class ResponseConfig(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'model_id',
            optional=True,
        ),

        RequestBodyAttribute(
            'prompt',
            optional=True,
        )
    ]


class LakeRequest(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'lookup_instructions',
            attribute_type=RequestAttributeType.OBJECT_LIST,
            supported_request_body_types=[BasicLookup, DirectLookup, RelatedRequestLookup, VectorLookup],
        ),

        RequestBodyAttribute(
            'processing_instructions',
            attribute_type=RequestAttributeType.OBJECT,
            supported_request_body_types=SummarizationAlgorithm,
        ),

        RequestBodyAttribute(
            'response_config',
            attribute_type=RequestAttributeType.OBJECT,
            supported_request_body_types=ResponseConfig,
            default={},
            optional=True,
        )
    ]

    path = '/lake_request'


class ScoreResponse(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'request_id',
        ),

        RequestBodyAttribute(
            'score',
            attribute_type=RequestAttributeType.FLOAT,
        ),

        RequestBodyAttribute(
            'score_comment',
            optional=True,
        )
    ]

    path = '/score_response'


class UpdateArchive(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'archive_id',
        ),

        RequestBodyAttribute(
            'description',
            optional=True,
        ),

        RequestBodyAttribute(
            'tag_hint_instructions',
            optional=True,
        )
    ]

    path = '/update_archive'


class UpdateEntry(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'content',
        ),

        RequestBodyAttribute(
            'entry_id',
        )
    ]

    path = '/update_entry'