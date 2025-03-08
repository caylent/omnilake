from os import path

from aws_cdk import (
    Duration,
)

from constructs import Construct

from da_vinci.core.resource_discovery import ResourceType

from da_vinci_cdk.stack import Stack

from da_vinci_cdk.constructs.access_management import ResourceAccessRequest
from da_vinci_cdk.constructs.base import resource_namer
from da_vinci_cdk.constructs.event_bus import EventBusSubscriptionFunction

from da_vinci_cdk.framework_stacks.services.event_bus.stack import EventBusStack

from omnilake.tables.jobs.stack import Job, JobsTable

from omnilake.tables.registered_request_constructs.cdk import (
    RegisteredRequestConstructObj,
    RequestConstructType,
    RegisteredRequestConstruct
)

from omnilake.tables.lake_chain_requests.stack import LakeChainRequestsTable, LakeChainRequest
from omnilake.tables.lake_requests.stack import LakeRequestsTable, LakeRequest
from omnilake.tables.registered_request_constructs.stack import RegisteredRequestConstructsTable

from omnilake.constructs.processors.inception.tables.chain_inception_runs.stack import (
    ChainInceptionRun,
    ChainInceptionRunsTable,
)

from omnilake.constructs.processors.inception.tables.inception_mutex.stack import (
    InceptionMutexTable,
    MutexLock,
)


from omnilake.constructs.processors.inception.schemas import InceptionProcessorSchema


class LakeConstructProcessorInceptionStack(Stack):
    def __init__(self, app_name: str, app_base_image: str, architecture: str,
                 deployment_id: str, stack_name: str, scope: Construct):
        """
        Inception processor stack for OmniLake.

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
                ChainInceptionRunsTable,
                EventBusStack,
                JobsTable,
                LakeChainRequestsTable,
                LakeRequestsTable,
                InceptionMutexTable,
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
            registered_type_name='INCEPTION',
            description='Executes a pseudo defined chain',
            schemas={
                "process": InceptionProcessorSchema.to_dict(),
            },
        )

        self.start_processor = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='omnilake-processor-inception-start',
            event_type=self.registered_request_construct_obj.get_operation_event_name('process'),
            description='Kicks off an inception processor.',
            entry=self.runtime_path,
            index='start.py',
            handler='handler',
            function_name=resource_namer('processor-inception-start', scope=self),
            memory_size=256,
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=ChainInceptionRun.table_name,
                    policy_name='read_write'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=Job.table_name,
                    policy_name='read_write'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=LakeRequest.table_name,
                    policy_name='read'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=LakeChainRequest.table_name,
                    policy_name='read_write'
                ),
            ],
            scope=self,
            timeout=Duration.minutes(5),
        )

        self.chain_complete = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='omnilake-processor-inception-chain-complete',
            event_type='omnilake_processor_inception_chain_complete',
            description='Handles the completion of an inception chain.',
            entry=self.runtime_path,
            index='completion.py',
            handler='handler',
            function_name=resource_namer('processor-inception-chain-complete', scope=self),
            memory_size=256,
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=ChainInceptionRun.table_name,
                    policy_name='read_write'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=Job.table_name,
                    policy_name='read_write'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=LakeRequest.table_name,
                    policy_name='read'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=LakeChainRequest.table_name,
                    policy_name='read_write'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=MutexLock.table_name,
                    policy_name='read_write'
                ),
            ],
            scope=self,
            timeout=Duration.minutes(1),
        )

        self.join_complete = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='omnilake-processor-inception-join-complete',
            event_type='omnilake_processor_inception_join_completion',
            description='Handles the final joining of any entries produced by fan out chains.',
            entry=self.runtime_path,
            index='join_complete.py',
            handler='handler',
            function_name=resource_namer('processor-inception-join-complete', scope=self),
            memory_size=256,
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=ChainInceptionRun.table_name,
                    policy_name='read_write'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=Job.table_name,
                    policy_name='read_write'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=LakeRequest.table_name,
                    policy_name='read'
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=LakeChainRequest.table_name,
                    policy_name='read_write'
                ),
            ],
            scope=self,
            timeout=Duration.minutes(8),
        )

        # Register Construct
        RegisteredRequestConstruct.from_definition(registered_construct=self.registered_request_construct_obj, scope=self)