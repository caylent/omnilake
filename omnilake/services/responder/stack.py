from os import path

from aws_cdk import (
    Duration,
    RemovalPolicy,
)

from aws_cdk.aws_iam import ManagedPolicy

from constructs import Construct

from da_vinci.core.resource_discovery import ResourceType

from da_vinci_cdk.stack import Stack

from da_vinci_cdk.constructs.access_management import ResourceAccessRequest
from da_vinci_cdk.constructs.base import resource_namer
from da_vinci_cdk.constructs.global_setting import GlobalSetting, SettingType
from da_vinci_cdk.constructs.event_bus import EventBusSubscriptionFunction
from da_vinci_cdk.constructs.lambda_function import LambdaFunction
from da_vinci_cdk.constructs.service import SimpleRESTService

from omnilake.tables.entries.stack import Entry, EntriesTable
from omnilake.tables.jobs.stack import Job, JobsTable
from omnilake.tables.information_requests.stack import (
    InformationRequest,
    InformationRequestsTable,
)
from omnilake.tables.compaction_jobs.stack import (
    CompactionJob,
    CompactionJobsTable,
)
from omnilake.tables.vector_store_chunks.stack import (
    VectorStoreChunksTable,
    VectorStoreChunk,
)

from omnilake.services.storage.stack import StorageManagerStack

class ResponderEngineStack(Stack):
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
                CompactionJobsTable,
                EntriesTable,
                JobsTable,
                InformationRequestsTable,
                StorageManagerStack,
                VectorStoreChunksTable,
            ],
            deployment_id=deployment_id,
            scope=scope,
            stack_name=stack_name,
        )

        base_dir = self.absolute_dir(__file__)

        self.runtime_path = path.join(base_dir, 'runtime')

        self.start_response_gen = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='response-start',
            event_type='start_information_request',
            description='Kick off the responder process.',
            entry=self.runtime_path,
            index='start.py',
            handler='start_responder',
            function_name=resource_namer('response-starter', scope=self),
            memory_size=512,
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=CompactionJob.table_name,
                    policy_name='read_write'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=InformationRequest.table_name,
                    policy_name='read_write'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=Job.table_name,
                    policy_name='read_write'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=VectorStoreChunk.table_name,
                    policy_name='read',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(5),
        )

        self.compaction_processor = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='compaction-processor',
            event_type='begin_compaction',
            description='Processes compaction requests.',
            entry=self.runtime_path,
            index='compactor.py',
            handler='compact_resources',
            function_name=resource_namer('compaction-processor', scope=self),
            memory_size=512,
            managed_policies=[
                ManagedPolicy.from_managed_policy_arn(
                    scope=self,
                    id='compaction-processor-amazon-bedrock-full-access',
                    managed_policy_arn='arn:aws:iam::aws:policy/AmazonBedrockFullAccess'
                ),
            ],
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_name='raw_storage_manager',
                    resource_type=ResourceType.REST_SERVICE,
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

        self.compaction_watcher = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='compaction-watcher',
            event_type='compaction_completed',
            description='Watches for compaction completion events.',
            entry=self.runtime_path,
            index='compactor.py',
            handler='compaction_watcher',
            function_name=resource_namer('compaction-watcher', scope=self),
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=CompactionJob.table_name,
                    policy_name='read_write'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=InformationRequest.table_name,
                    policy_name='read'
                ),
            ],
            scope=self,
            timeout=Duration.minutes(2),
        )

        self.final_response_generator = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='final-response-generator',
            event_type='final_response',
            description='Generates the final response for the request.',
            entry=self.runtime_path,
            index='response.py',
            handler='final_responder',
            function_name=resource_namer('final-responder', scope=self),
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
                    resource_name='raw_storage_manager',
                    resource_type=ResourceType.REST_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=CompactionJob.table_name,
                    policy_name='read_write'
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
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=InformationRequest.table_name,
                    policy_name='read_write'
                ),
            ],
            scope=self,
            timeout=Duration.minutes(2),
        )

        self.default_inclusive_sample_size = GlobalSetting(
            description='The default inclusive sample size for the responder.',
            namespace='responder',
            setting_key='default_inclusive_sample_size',
            setting_value=40,
            scope=self,
            setting_type=SettingType.INTEGER
        )

        self.default_max_entries = GlobalSetting(
            description='The default maximum number of entries to return.',
            namespace='responder',
            setting_key='default_max_entries',
            setting_value=100,
            scope=self,
            setting_type=SettingType.INTEGER
        )

        self.max_content_group_size = GlobalSetting(
            description='The maximum size a group of content can be for compaction purposes.',
            namespace='responder',
            setting_key='max_content_group_size',
            setting_value=5,
            scope=self,
            setting_type=SettingType.INTEGER
        )

        self.maximum_recursion_depth = GlobalSetting(
            description='The maximum recursive depth allowed for compaction.',
            namespace='responder',
            setting_key='compaction_maximum_recursion_depth',
            setting_value=4,
            scope=self,
            setting_type=SettingType.INTEGER
        )