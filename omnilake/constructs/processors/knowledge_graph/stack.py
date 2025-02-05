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

from omnilake.tables.jobs.stack import Job, JobsTable

from omnilake.tables.registered_request_constructs.cdk import (
    RegisteredRequestConstructObj,
    RequestConstructType,
    RegisteredRequestConstruct
)

from omnilake.tables.registered_request_constructs.stack import RegisteredRequestConstructsTable

from omnilake.constructs.processors.knowledge_graph.tables.knowledge_graph_jobs.stack import (
    KnowledgeGraphJob,
    KnowledgeGraphJobsTable,
)

from omnilake.services.ai_statistics_collector.stack import AIStatisticsCollectorStack
from omnilake.services.raw_storage_manager.stack import LakeRawStorageManagerStack

from omnilake.constructs.processors.knowledge_graph.schemas import KnowledgeGraphProcessorSchema


class LakeConstructProcessorKnowledgeGraphStack(Stack):
    def __init__(self, app_name: str, app_base_image: str, architecture: str,
                 deployment_id: str, stack_name: str, scope: Construct):
        """
        Knowledge graph processor stack for OmniLake.

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
            requires_event_bus=True,
            requires_exceptions_trap=True,
            required_stacks=[
                AIStatisticsCollectorStack,
                EventBusStack,
                JobsTable,
                KnowledgeGraphJobsTable,
                LakeRawStorageManagerStack,
                RegisteredRequestConstructsTable,
            ],
            deployment_id=deployment_id,
            scope=scope,
            stack_name=stack_name,
        )

        base_dir = self.absolute_dir(__file__)

        self.runtime_path = path.join(base_dir, 'runtime')

        self.registered_request_construct_obj = RegisteredRequestConstructObj(
            registered_construct_type=RequestConstructType.PROCESSOR,
            registered_type_name='KNOWLEDGE_GRAPH',
            description='Extracts knowledge from entries and attempts to extract value based on provided goal',
            schemas={
                "process": KnowledgeGraphProcessorSchema.to_dict(),
            },
        )

        self.start_processor = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='omnilake-processor-knowledge-graph-start',
            event_type=self.registered_request_construct_obj.get_operation_event_name('process'),
            description='Kicks off a knowledge Graph process.',
            entry=self.runtime_path,
            index='start.py',
            handler='handler',
            function_name=resource_namer('processor-knowledge-graph-start', scope=self),
            memory_size=256,
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=Job.table_name,
                    policy_name='read_write'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=KnowledgeGraphJob.table_name,
                    policy_name='read_write'
                ),
            ],
            scope=self,
            timeout=Duration.minutes(5),
        )

        self.extraction = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='omnilake-knowledge-graph-processor-extraction',
            event_type='omnilake_processor_knowledge_graph_extraction_request',
            description='Executes a knowledge Graph extraction.',
            entry=self.runtime_path,
            index='extraction.py',
            handler='handler',
            function_name=resource_namer('processor-knowledge-graph-extraction', scope=self),
            memory_size=512,
            managed_policies=[
                ManagedPolicy.from_managed_policy_arn(
                    scope=self,
                    id='extraction-amazon-bedrock-full-access',
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
                    resource_name=Job.table_name,
                    policy_name='read_write'
                ),
            ],
            scope=self,
            timeout=Duration.minutes(5),
        )

        self.extraction_complete = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='omnilake-knowledge-graph-processor-extraction-complete',
            event_type='omnilake_processor_knowledge_graph_extraction_complete',
            description='Handles the completion of a knowledge Graph extraction.',
            entry=self.runtime_path,
            index='extraction_complete.py',
            handler='handler',
            function_name=resource_namer('processor-knowledge-graph-extraction-complete', scope=self),
            memory_size=512,
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
                    resource_name=KnowledgeGraphJob.table_name,
                    policy_name='read_write'
                ),
            ],
            scope=self,
            timeout=Duration.minutes(8),
        )

        self.community_filtering = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='omnilake-knowledge-graph-processor-community-filtering',
            event_type='omnilake_processor_knowledge_graph_ai_filtering_request',
            description='Handles community filtering for the OmniLake knowledge graph processor.',
            entry=self.runtime_path,
            index='community_filtering.py',
            handler='handler',
            function_name=resource_namer('processor-knowledge-graph-filtering-execution', scope=self),
            memory_size=512,
            managed_policies=[
                ManagedPolicy.from_managed_policy_arn(
                    scope=self,
                    id='community-filtering-amazon-bedrock-full-access',
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
                    resource_name=Job.table_name,
                    policy_name='read_write'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=KnowledgeGraphJob.table_name,
                    policy_name='read_write'
                ),
            ],
            scope=self,
            timeout=Duration.minutes(6),
        )

        self.community_filtering_complete = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='omnilake-knowledge-graph-processor-community-filtering-complete',
            event_type='omnilake_processor_knowledge_graph_ai_filtering_complete',
            description='Handles the completion of a knowledge Graph community filtering.',
            entry=self.runtime_path,
            index='community_filtering_complete.py',
            handler='handler',
            function_name=resource_namer('processor-knowledge-graph-filtering-complete', scope=self),
            memory_size=512,
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
                    resource_name=KnowledgeGraphJob.table_name,
                    policy_name='read_write'
                ),
            ],
            scope=self,
            timeout=Duration.minutes(3),
        )

        self.response = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='omnilake-knowledge-graph-processor-response',
            event_type='omnilake_processor_knowledge_graph_final_response_request',
            description='Handles the final response for the OmniLake knowledge graph processor.',
            entry=self.runtime_path,
            index='response.py',
            handler='handler',
            function_name=resource_namer('processor-knowledge-graph-response', scope=self),
            memory_size=512,
            managed_policies=[
                ManagedPolicy.from_managed_policy_arn(
                    scope=self,
                    id='graph-response-amazon-bedrock-full-access',
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
                    resource_name=Job.table_name,
                    policy_name='read_write'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=KnowledgeGraphJob.table_name,
                    policy_name='read_write'
                ),
            ],
            scope=self,
            timeout=Duration.minutes(3),
        )

        # Register Construct
        RegisteredRequestConstruct.from_definition(registered_construct=self.registered_request_construct_obj, scope=self)