from dataclasses import dataclass
from enum import StrEnum
from typing import Optional

from anthropic import AnthropicBedrock

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
        self.anthropic = AnthropicBedrock()

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

        response = self.anthropic.messages.create(
            model=model_id,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            **invocation_kwargs
        )

        return AIInvocationResponse(
            response=response.content[0].text,
            statistics=AIInvocationStatistics(
                model_id=model_id,
                input_tokens=response.usage.input_tokens,
                output_tokens=response.usage.output_tokens,
            )
        )