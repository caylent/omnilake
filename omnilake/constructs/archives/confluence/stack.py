from os import path

from aws_cdk import Duration

from constructs import Construct

from aws_cdk.aws_iam import ManagedPolicy

from da_vinci.core.resource_discovery import ResourceType


from da_vinci_cdk.stack import Stack

from da_vinci_cdk.constructs.access_management import ResourceAccessRequest
from da_vinci_cdk.constructs.base import resource_namer
from da_vinci_cdk.constructs.event_bus import EventBusSubscriptionFunction

from da_vinci_cdk.framework_stacks.services.event_bus.stack import EventBusStack

from omnilake.tables.jobs.stack import Job, JobsTable
from omnilake.tables.indexed_entries.stack import IndexedEntry, IndexedEntriesTable 
from omnilake.tables.provisioned_archives.stack import Archive, ProvisionedArchivesTable
from omnilake.tables.sources.stack import Source, SourcesTable

from omnilake.tables.registered_request_constructs.cdk import (
    ArchiveConstructSchemas,
    RegisteredRequestConstruct, 
    RegisteredRequestConstructObj,
    RequestConstructType,
)

from omnilake.services.ai_statistics_collector.stack import AIStatisticsCollectorStack
from omnilake.services.raw_storage_manager.stack import LakeRawStorageManagerStack

from omnilake.tables.registered_request_constructs.stack import RegisteredRequestConstructsTable

from omnilake.constructs.archives.confluence.schemas import (
    ConfluenceArchiveLookupObjectSchema,
    ConfluenceArchiveProvisionObjectSchema,
)


class LakeConstructArchiveBasicStack(Stack):
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
                AIStatisticsCollectorStack,
                EventBusStack,
                JobsTable,
                IndexedEntriesTable,
                LakeRawStorageManagerStack,
                ProvisionedArchivesTable,
                RegisteredRequestConstructsTable,
                SourcesTable,
            ],
            deployment_id=deployment_id,
            scope=scope,
            stack_name=stack_name,
        )

        base_dir = self.absolute_dir(__file__)

        self.runtime_path = path.join(base_dir, 'runtime')

        schemas = ArchiveConstructSchemas(
            lookup=ConfluenceArchiveLookupObjectSchema,
            provision=ConfluenceArchiveProvisionObjectSchema,
        )

        self.registered_request_construct_obj = RegisteredRequestConstructObj(
            registered_construct_type=RequestConstructType.ARCHIVE,
            registered_type_name='CONFLUENCE',
            description='Confluence Archive Construct, looking in confluence space in realtime.',
            schemas=schemas,
            additional_supported_operations=set([]),
        )

        self.archive_provisioner = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='confluence_archive_provisioner',
            description='Provisions confluence archives.',
            entry=self.runtime_path,
            event_type=self.registered_request_construct_obj.get_operation_event_name('provision'),
            index='confluence-provisioner.py',
            handler='handler',
            function_name=resource_namer('confluence-archive-provisioner', scope=self),
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
                    resource_name=IndexedEntry.table_name,
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
            event_type='omnilake_archive_confluence_generate_entry_tags',
            index='generate_tags.py',
            handler='handler',
            function_name=resource_namer('archive-confluence-entry-tag-generator', scope=self),
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
                    resource_name='ai_statistics_collector',
                    resource_type=ResourceType.REST_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_name=Archive.table_name,
                    resource_type=ResourceType.TABLE,
                ),
                ResourceAccessRequest(
                    resource_name=IndexedEntry.table_name,
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



        self.data_retrieval = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='confluence_archive_data_retrieval',
            description='Retrieves data from a confluence archive.',
            entry=self.runtime_path,
            event_type=self.registered_request_construct_obj.get_operation_event_name('lookup'),
            index='lookup.py',
            handler='confluence_lookup',
            function_name=resource_namer('confluence-archive-data-retrieval', scope=self),
            memory_size=512,
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name=Job.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=IndexedEntry.table_name,
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
            timeout=Duration.minutes(5),
        )

        # Register the Basic Archive Construct
        RegisteredRequestConstruct.from_definition(registered_construct=self.registered_request_construct_obj, scope=self)
