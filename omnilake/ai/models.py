from abc import ABC, abstractmethod

from enum import StrEnum

class ModelIDs(StrEnum):
    """
    The ModelIDs class is used to store the supported model IDs.
    """
    HAIKU = 'anthropic.claude-3-5-haiku-20241022-v1:0'
    SONNET = 'anthropic.claude-3-7-sonnet-20250219-v1:0'

class AIModel(ABC):
    def __init__(self, model_id: str):
        self.model_id = model_id

    @abstractmethod
    def build_invocation_body(self, prompt: str, max_tokens: int = 2000, **invocation_kwargs) -> dict:
        pass

class AnthropicModel(AIModel):
    def __init__(self, model_id: str):
        super().__init__(model_id)

    def build_invocation_body(self, prompt: str, max_tokens: int = 2000, **invocation_kwargs) -> dict:
        body = invocation_kwargs or {}

        body["anthropic_version"] = "bedrock-2023-05-31"
        body["max_tokens"] = max_tokens

        if 'messages' not in body:
            body['messages'] = [{"role": "user", "content": prompt}]

        return body