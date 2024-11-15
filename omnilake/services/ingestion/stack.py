from os import path

from constructs import Construct

from aws_cdk import (
    Duration,
)
from aws_cdk.aws_iam import ManagedPolicy

from da_vinci.core.resource_discovery import ResourceType

from da_vinci_cdk.stack import Stack

from da_vinci_cdk.constructs.access_management import ResourceAccessRequest
from da_vinci_cdk.constructs.base import resource_namer
from da_vinci_cdk.constructs.event_bus import EventBusSubscriptionFunction
from da_vinci_cdk.constructs.global_setting import GlobalSetting, SettingType

from da_vinci_cdk.framework_stacks.event_bus.stack import EventBusStack

from omnilake.tables.archives.stack import Archive, ArchiveTable
from omnilake.tables.entries.stack import Entry, EntriesTable
from omnilake.tables.jobs.stack import Job, JobsTable
from omnilake.tables.sources.stack import Source, SourcesTable

from omnilake.services.storage.raw.stack import RawStorageManagerStack


class IngestionServiceStack(Stack):
    def __init__(self, app_name: str, app_base_image: str, architecture: str,
                 deployment_id: str, stack_name: str, scope: Construct):
        """
        Ingestion Service Stack. This service handles the initial ingestion of the data.

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
                ArchiveTable,
                EntriesTable,
                EventBusStack,
                JobsTable,
                SourcesTable,
                RawStorageManagerStack,
            ],
            deployment_id=deployment_id,
            scope=scope,
            stack_name=stack_name,
        )

        base_dir = self.absolute_dir(__file__)

        self.runtime_path = path.join(base_dir, 'runtime')

        self.inbound_processor = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='inbound-processor',
            event_type='add_entry',
            description='Processes the new entries and adds them to the storage.',
            entry=self.runtime_path,
            index='entry_creation.py',
            handler='handler',
            function_name=resource_namer('new-entry-processor', scope=self),
            memory_size=512,
            managed_policies=[
                ManagedPolicy.from_managed_policy_arn(
                    scope=self,
                    id='inbound-processor-amazon-bedrock-full-access',
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
                    resource_name=Archive.table_name,
                    policy_name='read',
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
                    resource_name=Source.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                )
            ],
            scope=self,
            timeout=Duration.minutes(5),
        )

        self.validate_content_uniqueness = GlobalSetting(
            description='Validates the content is unique before adding it to the storage. Uses the sha256 hash of the entry content.',
            namespace='ingestion',
            setting_key='enforce_content_uniqueness',
            setting_value='False',
            scope=self,
            setting_type=SettingType.BOOLEAN
        )