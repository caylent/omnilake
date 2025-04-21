from constructs import Construct

from da_vinci_cdk.stack import Stack
from da_vinci_cdk.constructs.ai import AIInferenceProfile

from omnilake.ai.models import ModelIDs

class AIStack(Stack):
    def __init__(self, app_name: str, deployment_id: str, scope: Construct,
                 stack_name: str):
        """
        Initialize the AIStack

        Keyword Arguments:
            app_name -- Name of the application
            deployment_id -- Identifier assigned to the installation
            scope -- Parent construct for the stack
            stack_name -- Name of the stack
        """
        super().__init__(
            app_name=app_name,
            deployment_id=deployment_id,
            scope=scope,
            stack_name=stack_name,
            requires_exceptions_trap=False,
        )

        self.haiku = AIInferenceProfile(
            scope=self,
            model_id=ModelIDs.HAIKU
        )

        self.sonnet = AIInferenceProfile(
            scope=self,
            model_id=ModelIDs.SONNET
        )