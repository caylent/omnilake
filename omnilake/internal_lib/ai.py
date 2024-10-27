import json
import logging

from dataclasses import dataclass
from enum import StrEnum
from typing import Optional

import boto3

from omnilake.tables.jobs.client import AIInvocationStatistics


class ModelIDs(StrEnum):
    """
    The ModelIDs class is used to store the model IDs.
    """
    HAIKU = "anthropic.claude-3-haiku-20240307-v1:0"
    SONNET = "anthropic.claude-3-5-sonnet-20240620-v1:0"
    VECTOR_EMBEDDINGS = "amazon.titan-embed-text-v2:0"


@dataclass
class AIInvocationResponse:
    """
    The AIInvocationResponse class is used to store the response from the AI invocation.
    """
    response: str
    statistics: AIInvocationStatistics


class AI:
    def __init__(self, default_model_id: str = ModelIDs.SONNET):
        """
        Initialize the AI service.
        """
        self.bedrock = boto3.client(service_name='bedrock-runtime')

        self.default_model_id = default_model_id

    def invoke(self, prompt: str, max_tokens: int = 2000, model_id: Optional[str] = None, **invocation_kwargs) -> AIInvocationResponse:
        """
        Invoke the AI model.

        Keyword Arguments:
            prompt: The prompt to invoke the AI model with.
            max_tokens: The maximum number of tokens to generate.
            model_id: The model ID to use.
            invocation_kwargs: The additional keyword arguments.

        Returns:
            AIInvocationResponse
        """
        if not model_id:
            model_id = self.default_model_id

        invocation_body = invocation_kwargs or {}

        invocation_body['anthropic_version'] = 'bedrock-2023-05-31'

        invocation_body['max_tokens'] = max_tokens

        invocation_body['messages'] = [{"role": "user", "content": prompt}]

        logging.info(f"Invoking Bedrock model {model_id} with: {invocation_body}")

        response = self.bedrock.invoke_model(
            modelId=model_id,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(invocation_body)
        )

        logging.info(f"Received response from Bedrock model {model_id}: {response}")

        response_body = json.loads(response['body'].read())

        return AIInvocationResponse(
            response=response_body['content'][0]['text'],
            statistics=AIInvocationStatistics(
                model_id=model_id,
                input_tokens=response_body['usage']['input_tokens'],
                output_tokens=response_body['usage']['output_tokens'],
            )
        )