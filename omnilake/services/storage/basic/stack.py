from os import path

from aws_cdk import Duration

from constructs import Construct

from aws_cdk.aws_iam import ManagedPolicy

from da_vinci.core.resource_discovery import ResourceType

from da_vinci_cdk.stack import Stack

from da_vinci_cdk.constructs.access_management import ResourceAccessRequest
from da_vinci_cdk.constructs.base import resource_namer
from da_vinci_cdk.constructs.event_bus import EventBusSubscriptionFunction

from da_vinci_cdk.framework_stacks.event_bus.stack import EventBusStack

from omnilake.tables.archives.stack import Archive, ArchiveTable
from omnilake.tables.jobs.stack import Job, JobsTable
from omnilake.tables.archive_entries.stack import ArchiveEntry, ArchiveEntriesTable
from omnilake.tables.sources.stack import Source, SourcesTable

from omnilake.services.storage.raw.stack import RawStorageManagerStack


class BasicArchiveManagerStack(Stack):
    def __init__(self, app_name: str, app_base_image: str, architecture: str,
                 deployment_id: str, stack_name: str, scope: Construct):
        """
        Basic Archive management stack for OmniLake.

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
                ArchiveEntriesTable,
                EventBusStack,
                JobsTable,
                RawStorageManagerStack,
                SourcesTable,
            ],
            deployment_id=deployment_id,
            scope=scope,
            stack_name=stack_name,
        )

        base_dir = self.absolute_dir(__file__)

        self.runtime_path = path.join(base_dir, 'runtime')

        self.archive_provisioner = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='basic_archive_provisioner',
            description='Provisions basic archives.',
            entry=self.runtime_path,
            event_type='create_basic_archive',
            index='provisioner.py',
            handler='handler',
            function_name=resource_namer('basic-archive-provisioner', scope=self),
            memory_size=256,
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name=Archive.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=Job.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=Source.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(1),
        )

        self.entry_tag_generator_event = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='entry_tag_generator',
            description='Generates tags for an entry.',
            entry=self.runtime_path,
            event_type='generate_entry_tags',
            index='generate_tags.py',
            handler='handler',
            function_name=resource_namer('entry-tag-generator', scope=self),
            memory_size=512,
            managed_policies=[
                ManagedPolicy.from_managed_policy_arn(
                    scope=self,
                    id='entry-tagger-amazon-bedrock-full-access',
                    managed_policy_arn='arn:aws:iam::aws:policy/AmazonBedrockFullAccess'
                ),
            ],
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_name=Archive.table_name,
                    resource_type=ResourceType.TABLE,
                ),
                ResourceAccessRequest(
                    resource_name=ArchiveEntry.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=Job.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(2),
        )

        self.entry_index_event = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='entry_basic_index_event',
            description='Indexes an entry into a basic archive.',
            entry=self.runtime_path,
            event_type='index_basic_entry',
            index='index.py',
            handler='handler',
            function_name=resource_namer('entry-basic-indexer', scope=self),
            memory_size=512,
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_name=Archive.table_name,
                    resource_type=ResourceType.TABLE,
                ),
                ResourceAccessRequest(
                    resource_name=ArchiveEntry.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=Job.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name='raw_storage_manager',
                    resource_type=ResourceType.REST_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_name=Source.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(2),
        )