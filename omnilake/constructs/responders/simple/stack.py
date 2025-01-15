from os import path

from aws_cdk import (
    Duration,
)

from aws_cdk.aws_iam import ManagedPolicy

from constructs import Construct

from da_vinci.core.resource_discovery import ResourceType

from da_vinci_cdk.stack import Stack

from da_vinci_cdk.constructs.access_management import ResourceAccessRequest
from da_vinci_cdk.constructs.base import resource_namer
from da_vinci_cdk.constructs.global_setting import GlobalSetting, GlobalSettingType
from da_vinci_cdk.constructs.event_bus import EventBusSubscriptionFunction

from da_vinci_cdk.framework_stacks.services.event_bus.stack import EventBusStack

from omnilake.tables.provisioned_archives.stack import Archive, ProvisionedArchivesTable
from omnilake.tables.entries.stack import Entry, EntriesTable
from omnilake.tables.jobs.stack import Job, JobsTable
from omnilake.tables.registered_request_constructs.cdk import (
    RegisteredRequestConstructObj,
    RequestConstructType,
    RegisteredRequestConstruct
)
from omnilake.tables.sources.stack import SourcesTable

from omnilake.services.ai_statistics_collector.stack import AIStatisticsCollectorStack

from omnilake.services.raw_storage_manager.stack import LakeRawStorageManagerStack

# Local Construct Imports
from omnilake.constructs.responders.simple.default_prompts import (
    DEFAULT_RESPONSE_PROMPT,
)

from omnilake.constructs.responders.simple.schemas import RequestBodySchema


class LakeConstructResponderSimpleStack(Stack):
    def __init__(self, app_name: str, app_base_image: str, architecture: str,
                 deployment_id: str, stack_name: str, scope: Construct):
        """
        Responder engine stack for OmniLake.

        Keyword Arguments:
            app_name: The name of the app.
            app_base_image: The base image for the app.
            architecture: The architecture of the app.
            deployment_id: The deployment ID.
            stack_name: The name of the stack.
            scope: The scope of the stack.
        """
        super().__init__(
            app_name=app_name,
            app_base_image=app_base_image,
            architecture=architecture,
            required_stacks=[
                AIStatisticsCollectorStack,
                EntriesTable,
                EventBusStack,
                JobsTable,
                LakeRawStorageManagerStack,
                ProvisionedArchivesTable,
                SourcesTable,
            ],
            deployment_id=deployment_id,
            scope=scope,
            stack_name=stack_name,
        )

        base_dir = self.absolute_dir(__file__)

        self.runtime_path = path.join(base_dir, 'runtime')

        self.registered_request_construct_obj = RegisteredRequestConstructObj(
            registered_construct_type=RequestConstructType.RESPONDER,
            registered_type_name='SIMPLE',
            description='Simple responder for OmniLake.',
            schemas={
                "respond": RequestBodySchema.to_dict(),
            },
        )

        self.response_generator = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='simple-response-generator',
            event_type=self.registered_request_construct_obj.get_operation_event_name('respond'),
            description='Generates a simple response for the request, given a stated goal.',
            entry=self.runtime_path,
            index='response.py',
            handler='final_responder',
            function_name=resource_namer('responder-simple-handler', scope=self),
            managed_policies=[
                ManagedPolicy.from_managed_policy_arn(
                    scope=self,
                    id='final-responder-amazon-bedrock-full-access',
                    managed_policy_arn='arn:aws:iam::aws:policy/AmazonBedrockFullAccess'
                ),
            ],
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_name='ai_statistics_collector',
                    resource_type=ResourceType.REST_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_name='raw_storage_manager',
                    resource_type=ResourceType.REST_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=Archive.table_name,
                    policy_name='read'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=Entry.table_name,
                    policy_name='read_write'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=Job.table_name,
                    policy_name='read_write'
                ),
            ],
            scope=self,
            timeout=Duration.minutes(5),
        )

        self.default_response_prompt = GlobalSetting(
            description='The default prompt for responses.',
            namespace='omnilake::simple_responder',
            setting_key='default_response_prompt',
            setting_value=DEFAULT_RESPONSE_PROMPT,
            scope=self,
            setting_type=GlobalSettingType.STRING
        )

        # Register the Construct
        RegisteredRequestConstruct.from_definition(registered_construct=self.registered_request_construct_obj, scope=self)
