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

from omnilake.tables.entries.stack import Entry, EntriesTable
from omnilake.tables.jobs.stack import Job, JobsTable

from omnilake.tables.registered_request_constructs.cdk import (
    RegisteredRequestConstructObj,
    RequestConstructType,
    RegisteredRequestConstruct
)
from omnilake.tables.registered_request_constructs.stack import RegisteredRequestConstructsTable
from omnilake.tables.sources.stack import (
    Source,
    SourcesTable,
)
from omnilake.constructs.processors.recursive_summarization.tables.summary_jobs.stack import (
    SummaryJob,
    SummaryJobsTable,
)

from omnilake.services.ai_statistics_collector.stack import AIStatisticsCollectorStack
from omnilake.services.raw_storage_manager.stack import LakeRawStorageManagerStack

from omnilake.constructs.processors.recursive_summarization.default_prompts import (
    DEFAULT_SUMMARY_PROMPT,
)

from omnilake.constructs.processors.recursive_summarization.schemas import RecursiveSummaryProcessor


class LakeConstructProcessorRecursiveSummarizationStack(Stack):
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
            requires_event_bus=True,
            requires_exceptions_trap=True,
            required_stacks=[
                AIStatisticsCollectorStack,
                EntriesTable,
                EventBusStack,
                JobsTable,
                LakeRawStorageManagerStack,
                RegisteredRequestConstructsTable,
                SourcesTable,
                SummaryJobsTable,
            ],
            deployment_id=deployment_id,
            scope=scope,
            stack_name=stack_name,
        )

        base_dir = self.absolute_dir(__file__)

        self.runtime_path = path.join(base_dir, 'runtime')

        self.registered_request_construct_obj = RegisteredRequestConstructObj(
            registered_construct_type=RequestConstructType.PROCESSOR,
            registered_type_name='SUMMARIZATION',
            description='Performs recursive summarization on content.',
            schemas={
                "process": RecursiveSummaryProcessor.to_dict(),
            },
        )

        self.start_processor = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='omnilake-summarization-processor-start',
            event_type=self.registered_request_construct_obj.get_operation_event_name('process'),
            description='Starts processing a summarization request.',
            entry=self.runtime_path,
            index='start.py',
            handler='handler',
            function_name=resource_namer('processor-recursive-summary-processor-start', scope=self),
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
                    resource_name=SummaryJob.table_name,
                    policy_name='read_write'
                ),
            ],
            scope=self,
            timeout=Duration.minutes(5),
        )

        self.summarization_processor = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='omnilake-summarization-processor-process',
            event_type='omnilake_processor_summarizer_summary_request',
            description='Processes summarization requests.',
            entry=self.runtime_path,
            index='summarizer.py',
            handler='handler',
            function_name=resource_namer('processor-recursive-summary-processor', scope=self),
            memory_size=512,
            managed_policies=[
                ManagedPolicy.from_managed_policy_arn(
                    scope=self,
                    id='summary-processor-amazon-bedrock-full-access',
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
                    resource_name=Source.table_name,
                    policy_name='read_write'
                ),
            ],
            scope=self,
            timeout=Duration.minutes(5),
        )

        self.summary_watcher = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='omnilake-summary-watcher',
            event_type='omnilake_processor_summarizer_summary_complete',
            description='Watches for summary completion events.',
            entry=self.runtime_path,
            index='watcher.py',
            handler='handler',
            function_name=resource_namer('processor-recursive-summary-watcher', scope=self),
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
                    resource_name=SummaryJob.table_name,
                    policy_name='read_write'
                ),
            ],
            scope=self,
            timeout=Duration.minutes(2),
        )

        self.default_summary_prompt = GlobalSetting(
            description='The default prompt for summarization.',
            namespace='omnilake::recursive_summarization_construct',
            setting_key='default_summary_prompt',
            setting_value=DEFAULT_SUMMARY_PROMPT,
            scope=self,
            setting_type=GlobalSettingType.STRING
        )

        self.max_content_group_size = GlobalSetting(
            description='The maximum size a group of content can be for summarization purposes.',
            namespace='omnilake::recursive_summarization_construct',
            setting_key='max_content_group_size',
            setting_value=5,
            scope=self,
            setting_type=GlobalSettingType.INTEGER
        )

        self.maximum_recursion_depth = GlobalSetting(
            description='The maximum recursive depth allowed for summarization.',
            namespace='omnilake::recursive_summarization_construct',
            setting_key='summary_maximum_recursion_depth',
            setting_value=4,
            scope=self,
            setting_type=GlobalSettingType.INTEGER
        )

        # Register Construct
        RegisteredRequestConstruct.from_definition(registered_construct=self.registered_request_construct_obj, scope=self)
