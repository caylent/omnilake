import json
import logging
import boto3

from dataclasses import asdict, dataclass
from typing import Optional

from da_vinci.core.resource_discovery import ResourceDiscovery, ResourceType
from omnilake.ai.models import AIModel, AnthropicModel, ModelIDs

logger = logging.getLogger(__name__)

@dataclass
class AIInvocationStatistics:
    input_tokens: int
    output_tokens: int
    model_id: str

    def to_dict(self):
        return asdict(self)


@dataclass
class AIInvocationResponse:
    """
    The AIInvocationResponse class is used to store the response from the AI invocation.
    """
    response: str
    statistics: AIInvocationStatistics


class AIInvocationClient:
    def __init__(self, default_model_id: str = ModelIDs.SONNET):
        """
        Initialize the AI service.
        """
        self.default_model_id = default_model_id
        self.bedrock = boto3.client(service_name='bedrock-runtime')

    def _resolve_model(self, model_id: str) -> AIModel:
        if 'anthropic' in model_id:
            return AnthropicModel(model_id=model_id)
        raise ValueError(f"Unsupported model ID: {model_id}")
    
    def _resolve_inference_profile_id(self, model_id: str) -> str:
        resource_discovery = ResourceDiscovery(
            resource_name=model_id,
            resource_type=ResourceType.LLM
        )
        try:
            return resource_discovery.endpoint_lookup()
        except Exception as e:
            logger.error(f"Failed to resolve inference profile ID for model {model_id}: {e}")
            return None

    def _internal_invoke(self, model_id: str, invocation_body: dict) -> AIInvocationResponse:
        logger.info(f"Invoking Bedrock model {model_id} with: {invocation_body}")

        response = self.bedrock.invoke_model(
            modelId=model_id,
            contentType="application/json",
            accept="application/json",
            body=json.dumps(invocation_body)
        )

        logger.info(f"Received response from Bedrock: {response}")

        response_body = json.loads(response['body'].read())

        return AIInvocationResponse(
            response=response_body['content'][0]['text'],
            statistics=AIInvocationStatistics(
                model_id=model_id,
                input_tokens=response_body['usage']['input_tokens'],
                output_tokens=response_body['usage']['output_tokens'],
            )
        )

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

        ai_model = self._resolve_model(model_id)

        actual_model_id = self._resolve_inference_profile_id(model_id) or model_id

        invocation_body = ai_model.build_invocation_body(prompt, max_tokens, **invocation_kwargs)

        return self._internal_invoke(actual_model_id, invocation_body)