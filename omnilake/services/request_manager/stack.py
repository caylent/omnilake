from os import path

from constructs import Construct

from aws_cdk import Duration

from aws_cdk.aws_iam import ManagedPolicy

from da_vinci.core.resource_discovery import ResourceType

from da_vinci_cdk.stack import Stack

from da_vinci_cdk.constructs.access_management import ResourceAccessRequest
from da_vinci_cdk.constructs.base import resource_namer
from da_vinci_cdk.constructs.event_bus import EventBusSubscriptionFunction

from omnilake.services.ai_statistics_collector.stack import AIStatisticsCollectorStack

from omnilake.services.raw_storage_manager.stack import LakeRawStorageManagerStack

from omnilake.tables.entries.stack import Entry, EntriesTable
from omnilake.tables.jobs.stack import Job, JobsTable
from omnilake.tables.lake_requests.stack import LakeRequest, LakeRequestsTable
from omnilake.tables.registered_request_constructs.stack import (
    RegisteredRequestConstruct,
    RegisteredRequestConstructsTable,
)
from omnilake.tables.sources.stack import Source, SourcesTable

from omnilake.tables.lake_chain_requests.stack import (
    LakeChainRequest,
    LakeChainRequestsTable,
)

from omnilake.services.request_manager.tables.lake_chain_coordinated_lake_requests.stack import (
    LakeChainCoordinatedLakeRequest,
    LakeChainCoordinatedLakeRequestsTable,
)


class LakeRequestManagerStack(Stack):
    def __init__(self, app_name: str, app_base_image: str, architecture: str,
                 deployment_id: str, stack_name: str, scope: Construct):
        """
        OmniLake Request Manager Service

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
                EntriesTable,
                JobsTable,
                LakeChainRequestsTable,
                LakeChainCoordinatedLakeRequestsTable,
                LakeRequestsTable,
                LakeRawStorageManagerStack,
                RegisteredRequestConstructsTable,
                SourcesTable,
            ],
            deployment_id=deployment_id,
            scope=scope,
            stack_name=stack_name,
        )

        base_dir = self.absolute_dir(__file__)

        self.runtime_path = path.join(base_dir, 'runtime')

        self.request_init = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='omnilake-lake-request',
            event_type='omnilake_lake_request',
            description='Initialize a request',
            entry=self.runtime_path,
            index='request_init.py',
            handler='handler',
            function_name=resource_namer('lake-request-init', scope=self),
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
                    resource_name=LakeRequest.table_name,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=RegisteredRequestConstruct.table_name,
                    policy_name='read',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(5),
        )

        self.stage_complete = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='omnilake-lake-request-stage-complete',
            event_type='omnilake_lake_request_internal_stage_response',
            description='Complete a request stage',
            entry=self.runtime_path,
            index='stage_complete.py',
            handler='handler',
            function_name=resource_namer('lake-request-stage-complete', scope=self),
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
                    resource_name=LakeRequest.table_name,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=RegisteredRequestConstruct.table_name,
                    policy_name='read',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(2),
        )

        self.primitive_lookup = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='omnilake-lake-request-primitive-lookup',
            event_type='omnilake_lake_request_primitive_lookup',
            description='Perform a primitive lookup',
            entry=self.runtime_path,
            index='primitive_lookup.py',
            handler='handler',
            function_name=resource_namer('lake-request-primitive-lookup', scope=self),
            memory_size=256,
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=Entry.table_name,
                    policy_name='read',
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=Job.table_name,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=LakeRequest.table_name,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=Source.table_name,
                    policy_name='read_write',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(2),
        )

        self.lookup_coordination = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='omnilake-lake-request-lookup-coordination',
            event_type='omnilake_lake_lookup_response',
            description='Coordinate a lookup',
            entry=self.runtime_path,
            index='lookup_coordination.py',
            handler='handler',
            function_name=resource_namer('lake-request-lookup-coordination', scope=self),
            memory_size=256,
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=LakeRequest.table_name,
                    policy_name='read_write',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(2),
        )

        self.catch_failures = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='omnilake-lake-request-handle-failures',
            event_type='omnilake_lake_request_failure',
            description='Catch all lake request failures',
            entry=self.runtime_path,
            index='failure.py',
            handler='handler',
            function_name=resource_namer('lake-request-failure', scope=self),
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
                    resource_name=LakeRequest.table_name,
                    policy_name='read_write',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(1),
        )

        ## Chain Management
        self.chain_init = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='omnilake-lake-request-chain-init',
            event_type='omnilake_chain_request',
            description='Initiate a chain',
            entry=self.runtime_path,
            index='chain_coordinator.py',
            handler='handle_initiate_chain',
            function_name=resource_namer('lake-request-chain-init', scope=self),
            memory_size=256,
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
                    resource_name=Job.table_name,
                    policy_name='read_write'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=LakeRequest.table_name,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=LakeChainRequest.table_name,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=LakeChainCoordinatedLakeRequest.table_name,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=RegisteredRequestConstruct.table_name,
                    policy_name='read',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(2),
        )

        self.chain_handle_lake_resp = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='omnilake-lake-request-request-complete',
            event_type='omnilake_lake_request_completion',
            description='Continue a chain off of a lake request completion',
            entry=self.runtime_path,
            index='chain_coordinator.py',
            handler='handle_lake_response',
            function_name=resource_namer('lake-request-chain-mgr', scope=self),
            memory_size=512,
            managed_policies=[
                ManagedPolicy.from_managed_policy_arn(
                    scope=self,
                    id='lake-response-amazon-bedrock-full-access',
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
                    resource_name='ai_statistics_collector',
                    resource_type=ResourceType.REST_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=Job.table_name,
                    policy_name='read_write'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=LakeRequest.table_name,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=LakeChainRequest.table_name,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=LakeChainCoordinatedLakeRequest.table_name,
                    policy_name='read_write',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(4),
        )