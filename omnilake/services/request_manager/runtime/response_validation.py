"""
Handles the a validation request
"""
import logging

from typing import Optional
from uuid import uuid4

from da_vinci.core.immutable_object import ObjectBody

from omnilake.internal_lib.ai import AI

from omnilake.internal_lib.clients import (
    AIStatisticSchema,
    AIStatisticsCollector,
    RawStorageManager,
)

from omnilake.tables.jobs.client import JobsClient
from omnilake.tables.lake_requests.client import LakeRequestsClient


class ValidationPrompt:
    BASE_PROMPT = """
    Given validation instructions, followed by content to review, analyze the content against the validation criteria. Determine if ALL validation criteria are met. 

Respond ONLY with:
- "SUCCESS" if content meets ALL validation criteria
- "FAILURE" if content fails ANY validation criteria"""

    def __init__(self, content: str, validation_prompt: str):
        """
        Initialize a ValidationPrompt object.

        Keyword Arguments:
        content -- The content to validate.
        validation_prompt -- The validation prompt.
        """
        self.content = content

        self.validation_prompt = validation_prompt

    def __str__(self):
        """
        Return the prompt as a string
        """
        return self.to_str()

    def to_str(self) -> str:
        """
        Return the prompt as a string
        """
        prompt_components = [
            self.BASE_PROMPT,
            "\nVALIDATION INSTRUCTIONS:",
            self.validation_prompt,
            "\nCONTENT TO VALIDATE:",
            self.content,
        ]

        return "\n\n".join(prompt_components)


def validate_response(lake_request_id: str, parent_job_id: str, parent_job_type: str, validation_instructions: str,
                      validation_model_id: Optional[str] = None) -> str:
    """
    Validate the response against the validation prompt.

    Keyword Arguments:
    lake_request_id -- The lake request ID.
    parent_job_id -- The parent job ID.
    parent_job_type -- The parent job type.
    validation_prompt -- The validation prompt.
    validation_model_id -- The model ID to use for validation.
    """
    jobs = JobsClient()

    parent_job = jobs.get(job_id=parent_job_id, job_type=parent_job_type)

    validation_job = parent_job.create_child(job_type="CHAIN_REQUEST_VALIDATION")

    with jobs.job_execution(job=validation_job) as job:
        lake_requests = LakeRequestsClient()

        lake_request = lake_requests.get(lake_request_id=lake_request_id)

        storage_manager = RawStorageManager()

        storage_resp = storage_manager.get_entry(entry_id=lake_request.response_entry_id)

        content = storage_resp.response_body["content"]

        if not content:
            raise ValueError(f"Content not found for entry ID: {lake_request.response_entry_id}")

        prompt = ValidationPrompt(content=content, validation_prompt=validation_instructions).to_str()

        logging.debug(f'Generated validation prompt: {prompt}')

        ai = AI()

        ai_response = ai.invoke(prompt=prompt, max_tokens=100, model_id=validation_model_id)

        logging.debug(f'Response result: {ai_response}')

        stats_collector = AIStatisticsCollector()

        invocation_id = str(uuid4())

        ai_statistic = ObjectBody(
            body={
                "invocation_id": invocation_id,
                "job_type": job.job_type,
                "job_id": job.job_id,
                "model_id": ai_response.statistics.model_id,
                "total_output_tokens": ai_response.statistics.output_tokens,
                "total_input_tokens": ai_response.statistics.input_tokens,
            },
            schema=AIStatisticSchema,
        )

        stats_collector.publish(statistic=ai_statistic)

        resp_str = ai_response.response

        status = resp_str.strip().upper()

        logging.debug(f'AI Response Status: {status}')

        if status not in ["SUCCESS", "FAILURE"]:
            raise ValueError("Invalid response status")

        return status