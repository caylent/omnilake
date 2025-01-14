import time

from datetime import datetime

from omnilake.client.client import OmniLake
from omnilake.client.request_definitions import (
    DescribeChainRequest,
    DescribeJob,
    DescribeLakeRequest,
    DirectResponseConfig,
    GetEntry,
    LakeChainStep,
    LakeChainValidationCondition,
    LakeChainValidation,
    LakeRequest,
    RelatedRequestResponseLookup,
    SimpleResponseConfig,
    SubmitChainRequest,
    SummarizationProcessor,
    VectorLookup,
)


def output_lake_response(omnilake: OmniLake, request_id: str, request_name: str):
    request_request_obj = DescribeLakeRequest(lake_request_id=request_id)

    resp = omnilake.request(request_request_obj)

    entry_id = resp.response_body['response_entry_id']

    content_resp = omnilake.request(GetEntry(entry_id=entry_id))

    entry_content = content_resp.response_body['content']

    print(f"Request \"{request_name}\" Response\n=================\n\n{entry_content}")


example_chain = SubmitChainRequest(
    chain=[
        LakeChainStep(
            name='omnilake_description',
            lake_request=LakeRequest(
                lookup_instructions=[
                    VectorLookup(
                        archive_id='omnilake',
                        max_entries=1,
                        query_string='what is OmniLake',
                        prioritize_tags=['readme']
                    ),
                ],
                processing_instructions=SummarizationProcessor(
                    goal='answer the question, what is OmniLake. be as detailed as possible',
                ),
                response_config=DirectResponseConfig(),
            ),
        ),
        LakeChainStep(
            name='omnilake_enterprise',
            lake_request=LakeRequest(
                lookup_instructions=[
                    RelatedRequestResponseLookup(
                        related_request_id='REF:omnilake_description.response_id',
                    ),
                ],
                processing_instructions=SummarizationProcessor(
                    goal='describe how OmniLake can be used to enable enterprises across all industries',
                ),
                response_config=SimpleResponseConfig(
                    goal='provide an executive summary of how OmniLake can be used to enable enterprises across all industries',
                ),
            ),
            validation=LakeChainValidation(
                on_success=LakeChainValidationCondition(
                    execute_chain_step='conditional_run',
                ),
                prompt='does the response provide a summary of how OmniLake can be used to enable enterprises',
            ),
        ),
        LakeChainStep(
            conditional=True,
            name='conditional_run',
            lake_request=LakeRequest(
                lookup_instructions=[
                    RelatedRequestResponseLookup(
                        related_request_id='REF:omnilake_description.response_id',
                    ),
                ],
                processing_instructions=SummarizationProcessor(
                    goal='if omnilake was an actual lake, what would the creatures that inhabit it, known as omnis, look like',
                ),
                response_config=DirectResponseConfig(),
            )
        ),
        LakeChainStep(
            # Conditional doesn't matter here, this inherits conditionality due to the reference to a conditional step
            name='omni_prompt',
            lake_request=LakeRequest(
                lookup_instructions=[
                    RelatedRequestResponseLookup(
                        related_request_id='REF:conditional_run.response_id',
                    ),
                ],
                processing_instructions=SummarizationProcessor(
                    goal="Return a prompt that instructs the model to explain how an Omni would live in the provided solution. It should include information about what an Omni is and instruct the model to be creative.\n\nDO NOT include your mention of the request, simply provide the response."
                ),
                response_config=DirectResponseConfig(),
            )
        ),
        LakeChainStep(
            name='omni_in_omnilake',
            lake_request=LakeRequest(
                lookup_instructions=[
                    RelatedRequestResponseLookup(
                        related_request_id='REF:omnilake_enterprise.response_id',
                    ),
                ],
                processing_instructions=SummarizationProcessor(
                    # Goal will be unused since the prompt is being overridden, but it's still required
                    goal='UNUSED',
                    prompt='REF:omni_prompt.response_body',
                ),
                response_config=DirectResponseConfig(),
            ),
        )
    ]
)


omnilake = OmniLake()

chain_create_resp = omnilake.request(example_chain)

job_type = chain_create_resp.response_body["job_type"]

job_id = chain_create_resp.response_body["job_id"]

chain_id = chain_create_resp.response_body["chain_request_id"]

job_describe = DescribeJob(
    job_id=job_id,
    job_type=job_type,
)

job_resp = omnilake.request(job_describe)

job_status = job_resp.response_body['status']

while job_status != 'COMPLETED':
    time.sleep(10)

    job_resp = omnilake.request(job_describe)

    if job_resp.response_body['status'] != job_status:
        job_status = job_resp.response_body['status']

        if job_status == 'FAILED':
            print(f'Job failed: {job_resp.response_body["status_message"]}')

            break

        print(f'Job status updated: {job_status}')

print(f'Final job status: {job_status}')

started = datetime.fromisoformat(job_resp.response_body['started'])

ended = datetime.fromisoformat(job_resp.response_body['ended'])

total_run_time = ended - started

print(f'Total run time: {total_run_time}')

chain_describe = DescribeChainRequest(
    chain_request_id=chain_id,
)

chain_resp = omnilake.request(chain_describe)

executed_requests = chain_resp.response_body["executed_requests"]

for request_name in executed_requests:
    output_lake_response(omnilake=omnilake, request_id=executed_requests[request_name], request_name=request_name)