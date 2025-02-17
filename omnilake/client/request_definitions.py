import logging

from typing import Dict, List, Optional, Union

from omnilake.client.client import (
    RequestAttributeType,
    RequestBodyAttribute,
    RequestBody,
)

from omnilake.client.construct_request_definitions import (
    BasicArchiveConfiguration,
    VectorArchiveConfiguration,
    BasicLookup,
    DirectEntryLookup,
    DirectResponseConfig,
    DirectSourceLookup,
    RelatedRequestResponseLookup,
    RelatedRequestSourcesLookup,
    SimpleResponseConfig,
    SummarizationProcessor,
    VectorLookup,
)


class AddEntry(RequestBody):
    """
    Add an entry to the lake. If the destination_archive is provided, the entry will be
    sent to be indexed in the archive.  It is up to the implementation of the archive as
    to when and how the entry is indexed.

    Keyword Arguments:
    content -- the content of the entry
    sources -- the sources used to generate the content
    destination_archive_id -- the archive to add the entry to
    effective_on -- the effective date of the entry
    original_of_source -- the source of the entry

    Example:
    ```
    AddEntry(
        content='This is a test entry',
        sources=['source1', 'source2'],
        archive_id='test_archive',
        effective_on='2021-01-01T00:00:00Z',
        original_of_source='source1',
    )
    ```
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'content',
        ),

        RequestBodyAttribute(
            'sources',
            attribute_type=RequestAttributeType.LIST,
        ),

        RequestBodyAttribute(
            'destination_archive_id',
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
    ]

    path = '/add_entry'

    def __init__(self, content: str, sources: list, destination_archive_id: Optional[str] = None,
                 effective_on: Optional[str] = None, original_of_source: Optional[str] = None):
        """
        Initialize the AddEntry request

        Keyword Arguments:
        content -- the content of the entry
        sources -- the sources used to generate the content
        archive_id -- the archive to add the entry to
        effective_on -- the effective date of the entry
        original_of_source -- the source of the entry

        Example:
        ```
        AddEntry(
            content='This is a test entry',
            sources=['source1', 'source2'],
            archive_id='test_archive',
            effective_on='2021-01-01T00:00:00Z',
            original_of_source='source1',
        )
        ```
        """
        super().__init__(
            content=content,
            sources=sources,
            destination_archive_id=destination_archive_id,
            effective_on=effective_on,
            original_of_source=original_of_source,
        )


class AddSource(RequestBody):
    """
    Adds a source to the lake

    Keyword Arguments:
    source_type -- the type of source
    source_arguments -- the arguments for the source

    Example:
    ```
    AddSource(
        source_type='test_source',
        source_arguments={
            'arg1': 'value1',
            'arg2': 'value2'
        }
    )
    ```
    """
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

    def __init__(self, source_type: str, source_arguments: Dict):
        """
        Initialize the AddSource request

        Keyword Arguments:
        source_type -- the type of source
        source_arguments -- the arguments for the source

        Example:
        ```
        AddSource(
            source_type='test_source',
            source_arguments={
                'arg1': 'value1',
                'arg2': 'value2'
            }
        )
        ```
        """
        super().__init__(
            source_type=source_type,
            source_arguments=source_arguments,
        )


class CreateArchive(RequestBody):
    """
    Create an archive in the lake

    Keyword Arguments:
    archive_id -- the id of the archive
    configuration -- the configuration of the archive
    description -- the description of the archive

    Example:
    ```
    CreateArchive(
        archive_id='test_archive',
        configuration=BasicArchiveConfiguration(
            archive_type='BASIC',
            source_types=['source1', 'source2'],
        ),
        description='This is a test archive'
    )
    ```
    """
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
        ),
    ]

    path = '/create_archive'

    def __init__(self, archive_id: str, configuration: Union[Dict, BasicArchiveConfiguration, VectorArchiveConfiguration],
                 description: str):
        """
        Initialize the CreateArchive request

        Keyword Arguments:
        archive_id -- the id of the archive
        configuration -- the configuration of the archive
        description -- the description of the archive

        Example:
        ```
        CreateArchive(
            archive_id='test_archive',
            configuration=BasicArchiveConfiguration(),
            description='This is a test archive'
        )
        ```
        """
        if not isinstance(configuration, dict):
            configuration = configuration.to_dict()

        super().__init__(
            archive_id=archive_id,
            configuration=configuration,
            description=description,
        )


class CreateSourceType(RequestBody):
    """
    Create a source type in the lake

    Keyword Arguments:
    name -- the name of the source type
    required_fields -- the required fields for the source type
    description -- the description of the source type

    Example:
    ```
    CreateSourceType(
        name='test_source_type',
        required_fields=['field1', 'field2'],
        description='This is a test source type'
    )
    ```
    """
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

    def __init__(self, name: str, required_fields: List, description: Optional[str] = None):
        """
        Initialize the CreateSourceType request

        Keyword Arguments:
        name -- the name of the source type
        required_fields -- the required fields for the source type
        description -- the description of the source type

        Example:
        ```
        CreateSourceType(
            name='test_source_type',
            required_fields=['field1', 'field2'],
            description='This is a test source type'
        )
        ```
        """
        super().__init__(
            name=name,
            required_fields=required_fields,
            description=description,
        )


class DescribeArchive(RequestBody):
    """
    Describe an archive in the lake

    Keyword Arguments:
    archive_id -- the id of the archive

    Example:
    ```
    DescribeArchive(
        archive_id='test_archive',
        archive_type='BASIC'
    )
    ```
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'archive_id',
        ),
    ]

    path = '/describe_archive'

    def __init__(self, archive_id: str):
        """
        Initialize the DescribeArchive request

        Keyword Arguments:
        archive_id -- the id of the archive

        Example:
        ```
        DescribeArchive(
            archive_id='test_archive',
        )
        ```
        """
        super().__init__(
            archive_id=archive_id,
        )


class DescribeEntry(RequestBody):
    """
    Describe an entry in the lake

    Keyword Arguments:
    entry_id -- the id of the entry

    Example:
    ```
    DescribeEntry(
        entry_id='test_entry'
    )
    ```
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'entry_id',
        )
    ]

    path = '/describe_entry'

    def __init__(self, entry_id: str):
        """
        Initialize the DescribeEntry request

        Keyword Arguments:
        entry_id -- the id of the entry

        Example:
        ```
        DescribeEntry(
            entry_id='test_entry'
        )
        ```
        """
        super().__init__(
            entry_id=entry_id,
        )


class DescribeJob(RequestBody):
    """
    Describe a job in the lake

    Keyword Arguments:
    job_id -- the id of the job
    job_type -- the type of the job

    Example:
    ```
    DescribeJob(
        job_id='test_job',
        job_type='test_job_type'
    )
    ```
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'job_id',
        ),

        RequestBodyAttribute(
            'job_type',
        )
    ]

    path = '/describe_job'

    def __init__(self, job_id: str, job_type: str):
        """
        Initialize the DescribeJob request

        Keyword Arguments:
        job_id -- the id of the job
        job_type -- the type of the job

        Example:
        ```
        DescribeJob(
            job_id='test_job',
            job_type='test_job_type'
        )
        ```
        """
        super().__init__(
            job_id=job_id,
            job_type=job_type,
        )


class DescribeSource(RequestBody):
    """
    Describe a source in the lake

    Keyword Arguments:
    source_id -- the id of the source
    source_type -- the type of the source

    Example:
    ```
    DescribeSource(
        source_id='test_source',
        source_type='test_source_type'
    )
    ```
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'source_id',
        ),

        RequestBodyAttribute(
            'source_type',
        )
    ]

    path = '/describe_source'

    def __init__(self, source_id: str, source_type: str):
        """
        Initialize the DescribeSource request

        Keyword Arguments:
        source_id -- the id of the source
        source_type -- the type of the source

        Example:
        ```
        DescribeSource(
            source_id='test_source',
            source_type='test_source_type'
        )
        ```
        """
        super().__init__(
            source_id=source_id,
            source_type=source_type,
        )


class DescribeSourceType(RequestBody):
    """
    Describe a source type in the lake

    Keyword Arguments:
    name -- the name of the source type

    Example:
    ```
    DescribeSourceType(
        name='test_source_type'
    )
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'name',
        )
    ]

    path = '/describe_source_type'

    def __init__(self, name: str):
        """
        Initialize the DescribeSourceType request

        Keyword Arguments:
        name -- the name of the source type

        Example:
        ```
        DescribeSourceType(
            name='test_source_type'
        )
        """
        super().__init__(
            name=name,
        )


class DescribeChainRequest(RequestBody):
    """
    Describe a chain request in the lake

    Keyword Arguments:
    request_id -- the id of the request

    Example:
    ```
    DescribeChainRequest(
        request_id='test_request'
    )
    ```
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'chain_request_id',
        )
    ]

    path = '/describe_chain_request'

    def __init__(self, chain_request_id: str):
        """
        Initialize the DescribeChainRequest request

        Keyword Arguments:
        chain_request_id -- the id of the request

        Example:
        ```
        DescribeChainRequest(
            chain_request_id='test_request'
        )
        ```
        """
        super().__init__(
            chain_request_id=chain_request_id,
        )


class DescribeLakeRequest(RequestBody):
    """
    Describe a lake request in the lake

    Keyword Arguments:
    request_id -- the id of the request

    Example:
    ```
    DescribeLakeRequest(
        request_id='test_request'
    )
    ```
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'lake_request_id',
        )
    ]

    path = '/describe_lake_request'

    def __init__(self, lake_request_id: str):
        """
        Initialize the DescribeLakeRequest request

        Keyword Arguments:
        lake_request_id -- the id of the request

        Example:
        ```
        DescribeLakeRequest(
            lake_request_id='test_request'
        )
        """
        super().__init__(
            lake_request_id=lake_request_id,
        )


class GetEntry(RequestBody):
    """
    Get an entry from the lake

    Keyword Arguments:
    entry_id -- the id of the entry

    Example:
    ```
    GetEntry(
        entry_id='test_entry'
    )
    ```
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'entry_id',
        )
    ]

    path = '/get_entry'

    def __init__(self, entry_id: str):
        """
        Initialize the GetEntry request

        Keyword Arguments:
        entry_id -- the id of the entry

        Example:
        ```
        GetEntry(
            entry_id='test_entry'
        )
        """
        super().__init__(
            entry_id=entry_id,
        )


class IndexEntry(RequestBody):
    """
    Index an existing entry into an existing archive

    Keyword Arguments:
    archive_id -- the id of the archive
    entry_id -- the id of the entry

    Example:
    ```
    IndexEntry(
        archive_id='test_archive',
        entry_id='test_entry'
    )
    ```
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'archive_id',
        ),

        RequestBodyAttribute(
            'entry_id',
        )
    ]

    path = '/index_entry'

    def __init__(self, archive_id: str, entry_id: str):
        """
        Initialize the IndexEntry request

        Keyword Arguments:
        archive_id -- the id of the archive
        entry_id -- the id of the entry

        Example:
        ```
        IndexEntry(
            archive_id='test_archive',
            entry_id='test_entry'
        )
        """
        super().__init__(
            archive_id=archive_id,
            entry_id=entry_id,
        )


class LakeRequest(RequestBody):
    """
    Submit a request to the lake

    Keyword Arguments:
    lookup_instructions -- the lookup instructions for the request
    processing_instructions -- the processing instructions for the request
    response_config -- the response configuration for the request

    Example:
    ```
    SubmitLakeRequest(
        lookup_instructions=[
            BasicLookup(
                archive_id='test_archive',
                max_entries=20,
            ),
        ],
        processing_instructions=SummarizationProcessor(
            include_source_metadata=True,
        ),
        response_config=SimpleResponseConfig(
            destination_archive_id='test_destination_archive'
            goal='What was the result of ...'
        )
    )
    ```
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'lookup_instructions',
            attribute_type=RequestAttributeType.OBJECT_LIST,
            supported_request_body_types=[BasicLookup, DirectEntryLookup, DirectSourceLookup, RelatedRequestResponseLookup, RelatedRequestSourcesLookup, VectorLookup],
        ),

        # Name is used as a reference for chained requests
        RequestBodyAttribute(
            'name',
            attribute_type=RequestAttributeType.STRING,
            optional=True,
        ),

        RequestBodyAttribute(
            'processing_instructions',
            attribute_type=RequestAttributeType.OBJECT,
            supported_request_body_types=[SummarizationProcessor],
        ),

        RequestBodyAttribute(
            'response_config',
            attribute_type=RequestAttributeType.OBJECT,
            supported_request_body_types=[DirectResponseConfig, SimpleResponseConfig],
        ),
    ]

    def __init__(self, lookup_instructions: List[Union[Dict, BasicLookup, DirectEntryLookup, DirectSourceLookup, RelatedRequestResponseLookup, RelatedRequestSourcesLookup, VectorLookup]],
                    processing_instructions: Union[Dict, SummarizationProcessor],
                    response_config: Optional[Union[Dict, DirectResponseConfig, SimpleResponseConfig]] = None):
            """
            Initialize the LakeRequest Object
    
            Keyword Arguments:
            name -- the name of the request, this is used as a local reference for chained requests
            lookup_instructions -- the lookup instructions for the request
            processing_instructions -- the processing instructions for the request
            response_config -- the response configuration for the request
    
            Example:
            ```
            SubmitLakeRequest(
                lookup_instructions=[
                    BasicLookup(
                        archive_id='test_archive',
                        max_entries=20,
                    ),
                ],
                processing_instructions=SummarizationProcessor(
                    include_source_metadata=True,
                ),
                response_config=SimpleResponseConfig(
                    destination_archive_id='test_destination_archive'
                    goal='What was the result of ...'
                )
            )
            """
            flt_lookup_instructions = []

            for lookup_instruction in lookup_instructions:
                if isinstance(lookup_instruction, RequestBody):
                    lookup_instruction = lookup_instruction.to_dict()

                flt_lookup_instructions.append(lookup_instruction)

            flt_processing_instructions = processing_instructions

            if isinstance(processing_instructions, RequestBody):
                flt_processing_instructions = processing_instructions.to_dict()
    
            flt_response_config = response_config or {}

            if response_config and isinstance(response_config, RequestBody):
                flt_response_config = response_config.to_dict()
    
            super().__init__(
                lookup_instructions=flt_lookup_instructions,
                processing_instructions=flt_processing_instructions,
                response_config=flt_response_config,
            )


class LakeChainValidationCondition(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'execute_chain_step',
            attribute_type=RequestAttributeType.STRING,
            optional=True,
        ),

        RequestBodyAttribute(
            'terminate_chain',
            attribute_type=RequestAttributeType.BOOLEAN,
            default=False,
            optional=True,
        ),
    ]

    def __init__(self, execute_chain_step: Optional[str] = None, terminate_chain: Optional[bool] = None):
        """
        Initialize the LakeChainValidationBody Object

        Keyword Arguments:
        execute_chain_step -- the name of the chain step to execute when condition is met
        terminate_chain -- whether to terminate the chain
        """
        if not execute_chain_step and terminate_chain is None:
            raise ValueError('Either execute_chain_step or terminate_chain must be provided')

        super().__init__(
            execute_chain_step=execute_chain_step,
            terminate_chain=terminate_chain,
        )


class LakeChainValidation(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'model_id',
            attribute_type=RequestAttributeType.STRING,
            optional=True,
        ),

        RequestBodyAttribute(
            'on_failure',
            attribute_type=RequestAttributeType.OBJECT,
            supported_request_body_types=[LakeChainValidationCondition],
            optional=True,
        ),

        RequestBodyAttribute(
            'on_success',
            attribute_type=RequestAttributeType.OBJECT,
            supported_request_body_types=[LakeChainValidationCondition],
            optional=True,
        ),

        RequestBodyAttribute(
            'prompt',
            attribute_type=RequestAttributeType.STRING,
        ),
    ]

    def __init__(self, model_id: Optional[str] = None, on_failure: Optional[Union[Dict, LakeChainValidationCondition]] = None,
                 on_success: Optional[Union[Dict, LakeChainValidationCondition]] = None, prompt: str = None):
            """
            Initialize the LakeRequestValidation Object
    
            Keyword Arguments:
            model_id -- the id of the model to use for validation
            on_failure -- the condition to execute on failure
            on_success -- the condition to execute on success
            prompt -- the prompt for the validation
            """
            if not on_failure and not on_success:
                logging.warning(' neither on_failure or on_success were provided, the validation results will have no effect')
    
            super().__init__(
                model_id=model_id,
                on_failure=on_failure,
                on_success=on_success,
                prompt=prompt,
            )


class LakeChainStep(RequestBody):
    """
    A step in a lake chain
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'conditional',
            attribute_type=RequestAttributeType.BOOLEAN,
            optional=True,
            default=False,
        ),

        RequestBodyAttribute(
            'lake_request',
            attribute_type=RequestAttributeType.OBJECT,
            supported_request_body_types=[LakeRequest],
        ),

        RequestBodyAttribute(
            'name',
            attribute_type=RequestAttributeType.STRING,
        ),

        RequestBodyAttribute(
            'validation',
            attribute_type=RequestAttributeType.OBJECT,
            supported_request_body_types=[LakeChainValidation],
            optional=True,
        ),
    ]

    def __init__(self, lake_request: Union[Dict, LakeRequest], name: str, conditional: Optional[bool] = False,
                 validation: Optional[Union[Dict, LakeChainValidation]] = None):
            """
            Initialize the LakeChainStep Object
    
            Keyword Arguments:
            conditional -- whether the step is conditional
            lake_request -- the lake request for the step
            name -- the name of the step
            validation -- the validation for the step
            """

            super().__init__(
                conditional=conditional,
                lake_request=lake_request,
                name=name,
                validation=validation,
            )


class ListEntries(RequestBody):
    """
    List entries in the lake

    Keyword Arguments:
    archive_id -- the id of the archive
    limit -- the limit of entries to return
    next_token -- the next token for pagination

    Example:
    ```
    ListEntries(
        archive_id='test_archive',
        limit=25,
        next_token='next_token'
    )
    ```
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'archive_id',
            optional=True,
        ),

        RequestBodyAttribute(
            'limit',
            attribute_type=RequestAttributeType.INTEGER,
            default=25,
            optional=True,
        ),

        RequestBodyAttribute(
            'next_token',
            optional=True,
        )
    ]

    path = '/list_entries'

    def __init__(self, archive_id: Optional[str] = None, limit: Optional[int] = 25, next_token: Optional[str] = None):
        """
        Initialize the ListEntries request

        Keyword Arguments:
        archive_id -- the id of the archive
        limit -- the limit of entries to return
        next_token -- the next token for pagination

        Example:
        ```
        ListEntries(
            archive_id='test_archive',
            limit=25,
            next_token='next_token'
        )
        """
        super().__init__(
            archive_id=archive_id,
            limit=limit,
            next_token=next_token,
        )


class ListProvisionedArchives(RequestBody):
    """
    List provisioned archives in the lake

    Keyword Arguments:
    limit -- the limit of archives to return
    next_token -- the next token for pagination

    Example:
    ```
    ListProvisionedArchives(
        limit=25,
        next_token='next_token'
    )
    ```
    """
    attribute_definitions = [
        RequestBodyAttribute(
            'limit',
            attribute_type=RequestAttributeType.INTEGER,
            default=25,
            optional=True,
        ),

        RequestBodyAttribute(
            'next_token',
            optional=True,
        )
    ]
    path = '/list_archives'


class SubmitChainRequest(RequestBody):
    attribute_definitions = [
        RequestBodyAttribute(
            'chain',
            attribute_type=RequestAttributeType.OBJECT_LIST,
            supported_request_body_types=[LakeRequest],
        )   
    ]

    path = '/submit_chain_request'

    def __init__(self, chain: List[Union[Dict, LakeRequest]]):
        """
        Initialize the SubmitLakeRequestChain request

        Keyword Arguments:
        chain -- the chain of lake requests to submit
        """
        flt_requests = []

        for request in chain:
            if isinstance(request, RequestBody):
                request = request.to_dict()

            flt_requests.append(request)

        super().__init__(chain=flt_requests)


class SubmitLakeRequest(LakeRequest):
    path = '/submit_lake_request'

    def __init__(self, lookup_instructions: List[Union[Dict, BasicLookup, DirectEntryLookup, DirectSourceLookup, RelatedRequestResponseLookup, RelatedRequestSourcesLookup, VectorLookup]],
                    processing_instructions: Union[Dict, SummarizationProcessor],
                    response_config: Optional[Union[Dict, SimpleResponseConfig]] = None):
        """
        Initialize the SubmitLakeRequest request

        Keyword Arguments:
        lookup_instructions -- the lookup instructions for the request
        processing_instructions -- the processing instructions for the request
        response_config -- the response configuration for the request

        Example:
        ```
        SubmitLakeRequest(
            lookup_instructions=[
                BasicLookup(
                    archive_id='test_archive',
                    max_entries=20,
                ),
            ],
            processing_instructions=SummarizationProcessor(
                include_source_metadata=True,
            ),
            response_config=SimpleResponseConfig(
                destination_archive_id='test_destination_archive'
                goal='What was the result of ...'
            )
        )
        """

        super().__init__(
            lookup_instructions=lookup_instructions,
            processing_instructions=processing_instructions,
            response_config=response_config,
        )