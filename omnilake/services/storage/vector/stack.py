from os import path

from aws_cdk import (
    Duration,
    RemovalPolicy,
)

from constructs import Construct

from aws_cdk import aws_events as cdk_events
from aws_cdk import aws_events_targets as cdk_events_targets
from aws_cdk.aws_iam import ManagedPolicy
from aws_cdk.aws_s3 import Bucket, BucketEncryption

from da_vinci.core.resource_discovery import ResourceType

from da_vinci_cdk.stack import Stack

from da_vinci_cdk.constructs.access_management import ResourceAccessRequest
from da_vinci_cdk.constructs.base import resource_namer
from da_vinci_cdk.constructs.global_setting import GlobalSetting, SettingType
from da_vinci_cdk.constructs.event_bus import EventBusSubscriptionFunction
from da_vinci_cdk.constructs.lambda_function import LambdaFunction


from omnilake.tables.archives.stack import Archive, ArchiveTable
from omnilake.tables.archive_entries.stack import ArchiveEntry, ArchiveEntriesTable
from omnilake.tables.entries.stack import Entry, EntriesTable
from omnilake.tables.jobs.stack import Job, JobsTable
from omnilake.tables.sources.stack import Source, SourcesTable
from omnilake.tables.vector_stores.stack import VectorStoresTable, VectorStore
from omnilake.tables.vector_store_chunks.stack import VectorStoreChunk, VectorStoreChunksTable
from omnilake.tables.vector_store_tags.stack import VectorStoreTag, VectorStoreTagsTable
from omnilake.tables.vector_store_queries.stack import VectorStoreQueriesTable, VectorStoreQuery

from omnilake.services.storage.raw.stack import RawStorageManagerStack


class VectorArchiveManagerStack(Stack):
    def __init__(self, app_name: str, app_base_image: str, architecture: str,
                 deployment_id: str, stack_name: str, scope: Construct):
        """
        Vector Archive manager stack for OmniLake.

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
                EntriesTable,
                JobsTable,
                RawStorageManagerStack,
                SourcesTable,
                VectorStoresTable,
                VectorStoreChunksTable,
                VectorStoreQueriesTable,
                VectorStoreTagsTable,
            ],
            deployment_id=deployment_id,
            scope=scope,
            stack_name=stack_name,
        )

        base_dir = self.absolute_dir(__file__)

        self.runtime_path = path.join(base_dir, 'runtime')

        self.vector_store_bucket = Bucket(
            self,
            'vector-store-bucket',
            encryption=BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.vector_store_bucket_setting = GlobalSetting(
            namespace='vector_storage',
            setting_key='vector_store_bucket',
            setting_value=self.vector_store_bucket.bucket_name,
            scope=self,
        )

        self.storage_provisioner = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='vector_archive_provisioner',
            description='Provisions vector archives.',
            entry=self.runtime_path,
            event_type='create_vector_archive',
            index='provisioner.py',
            handler='handler',
            function_name=resource_namer('vector-archive-provisioner', scope=self),
            memory_size=512,
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
                    resource_name=VectorStore.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(2),
        )

        self.vector_store_bucket.grant_read_write(self.storage_provisioner.handler.function)

        self.entry_index_event = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='entry_vector_index_event',
            description='Indexes an entry into a vector store for the requested archive.',
            entry=self.runtime_path,
            event_type='index_vector_entry',
            index='index.py',
            handler='handler',
            function_name=resource_namer('entry-vector-indexer', scope=self),
            memory_size=512,
            managed_policies=[
                ManagedPolicy.from_managed_policy_arn(
                    scope=self,
                    id='entry-vector-index-amazon-bedrock-full-access',
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
                    policy_name='read',
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
                    resource_name=Source.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read',
                ),
                ResourceAccessRequest(
                    resource_name='raw_storage_manager',
                    resource_type=ResourceType.REST_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_name=VectorStore.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=VectorStoreChunk.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=VectorStoreTag.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                )
            ],
            scope=self,
            timeout=Duration.minutes(2),
        )

        self.vector_store_bucket.grant_read_write(self.entry_index_event.handler.function)

        self.vector_tag_recalculator = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='vector_tag_calculator',
            description='Recalculates the tags for a vector store.',
            entry=self.runtime_path,
            event_type='recalculate_vector_tags',
            index='recalculate_vector_tags.py',
            handler='handler',
            function_name=resource_namer('vector-tag-recalculator', scope=self),
            memory_size=1024,
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_name=Entry.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read',
                ),
                ResourceAccessRequest(
                    resource_name=Job.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=VectorStore.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=VectorStoreTag.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                )
            ],
            scope=self,
            timeout=Duration.minutes(10),
        )

        self.vector_store_bucket.grant_read_write(self.vector_tag_recalculator.handler.function)

        self.recalculation_check = LambdaFunction(
            base_image=self.app_base_image,
            construct_id='recalculation_check',
            description='Runs a check to see if any vector store tags need to be recalculated.',
            entry=self.runtime_path,
            function_name=resource_namer('vector-tag-recalculator-check', self),
            index='recalculate_checker.py',
            handler='recalculate_checker',
            scope=self,
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
                    resource_name=VectorStore.table_name,
                    policy_name='read'
                ),
            ],
        )

        recalculation_rule = cdk_events.Rule(
            scope=self,
            id='recalculation-rule',
            schedule=cdk_events.Schedule.cron(hour='16', minute='0'),
        )

        recalculation_rule.add_target(
            cdk_events_targets.LambdaFunction(self.recalculation_check.function)
        )

        self.chunk_overlap_setting = GlobalSetting(
            description="The percentage of overlap between chunks in a vector store.",
            namespace='vector_storage',
            setting_key='chunk_overlap',
            setting_value=40,
            scope=self,
            setting_type=SettingType.INTEGER
        )

        self.max_chunk_length_setting = GlobalSetting(
            description="The maximum length of a chunk in a vector store.",
            namespace='vector_storage',
            setting_key='max_chunk_length',
            setting_value=1500,
            scope=self,
            setting_type=SettingType.INTEGER
        )

        self.max_entries_per_vector = GlobalSetting(
            description="The maximum number of entries that should be stored in a vector store.",
            namespace='vector_storage',
            setting_key='max_entries_per_vector',
            setting_value=1000,
            scope=self,
            setting_type=SettingType.INTEGER
        )

        self.max_entries_threshold = GlobalSetting(
            description="The threshold percentage used to calculate when a vector store is nearing its maximum entry count and should be rebalanced.",
            namespace='vector_storage',
            setting_key='max_entries_rebalance_threshold',
            setting_value=20,
            scope=self,
            setting_type=SettingType.INTEGER
        )

        self.max_vector_store_search_group_size = GlobalSetting(
            description="The maximum number of vector stores to search for a single vs_query execution.",
            namespace='vector_storage',
            setting_key='max_vector_store_search_group_size',
            setting_value=10,
            scope=self,
            setting_type=SettingType.INTEGER
        )

        self.maintenance_delay_interval = GlobalSetting(
            description="The delay interval to use when archive is under maintenance.",
            namespace='vector_storage',
            setting_key='query_delay',
            setting_value=600,
            scope=self,
            setting_type=SettingType.INTEGER
        )

        self.tag_recalculation_frequency = GlobalSetting(
            description="The frequency at which to recalculate vector store tags. (in days)",
            namespace='vector_storage',
            setting_key='tag_recalculation_frequency',
            setting_value=7,
            scope=self,
            setting_type=SettingType.INTEGER
        )

        self.rebalance_top_tags_percentage = GlobalSetting(
            description="The percentage of top tags to use when rebalancing a vector store.",
            namespace='vector_storage',
            setting_key='rebalance_top_tags_percentage',
            setting_value=10,
            scope=self,
            setting_type=SettingType.INTEGER
        )

        self.rebalance_tag_match_threshold_percentage = GlobalSetting(
            description="The percentage of tags that must match an entry when rebalancing a vector store.",
            namespace='vector_storage',
            setting_key='rebalance_tag_match_threshold_percentage',
            setting_value=40,
            scope=self,
            setting_type=SettingType.INTEGER
        )

        ## Maintenance Mode Events
        self.maintenance_mode_begin = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='begin_maintenance_mode',
            description='Put an archive into maintenance mode.',
            entry=self.runtime_path,
            event_type='begin_maintenance_mode',
            index='maintenance.py',
            handler='begin_maintenance_mode',
            function_name=resource_namer('begin-archive-maintenance', scope=self),
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name=Archive.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(1),
        )

        self.maintenance_mode_end = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='end_maintenance_mode',
            description='Take an archive out of maintenance mode.',
            entry=self.runtime_path,
            event_type='end_maintenance_mode',
            index='maintenance.py',
            handler='end_maintenance_mode',
            function_name=resource_namer('end-archive-maintenance', scope=self),
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
                )
            ],
            scope=self,
            timeout=Duration.minutes(1),
        )

        self.rebalancer_check = LambdaFunction(
            base_image=self.app_base_image,
            construct_id='rebalancer_check',
            description='Runs a check to see if any vector stores need to be rebalanced.',
            entry=self.runtime_path,
            function_name=resource_namer('vector-rebalancer-check', self),
            index='vector_rebalancer_checker.py',
            handler='rebalance_checker',
            memory_size=512,
            scope=self,
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
                    resource_name=VectorStore.table_name,
                    policy_name='read'
                ),
            ],
        )

        rebalancer_rule = cdk_events.Rule(
            scope=self,
            id='rebalancer-rule',
            schedule=cdk_events.Schedule.cron(hour='20', minute='0'),
        )

        rebalancer_rule.add_target(
            cdk_events_targets.LambdaFunction(self.rebalancer_check.function)
        )

        self.vector_store_rebalancer = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='vector_store_rebalancer',
            description='Rebalances a vector store.',
            entry=self.runtime_path,
            event_type='vector_store_rebalancing',
            index='vector_rebalancer.py',
            handler='handler',
            function_name=resource_namer('vector-rebalancer', scope=self),
            memory_size=1024,
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_name=Entry.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read',
                ),
                ResourceAccessRequest(
                    resource_name=Job.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=VectorStore.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=VectorStoreChunk.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=VectorStoreTag.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                )
            ],
            scope=self,
            timeout=Duration.minutes(15),
        )

        self.vector_store_bucket.grant_read_write(self.vector_store_rebalancer.handler.function)

        self.query_request = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='vector_store_query_request',
            description='Handles a query request for a vector store based archive.',
            entry=self.runtime_path,
            event_type='query_request',
            index='query_request.py',
            handler='handler',
            function_name=resource_namer('vector-store-query-request', scope=self),
            memory_size=256,
            managed_policies=[
                ManagedPolicy.from_managed_policy_arn(
                    scope=self,
                    id='query-request-amazon-bedrock-full-access',
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
                    policy_name='read',
                ),
                ResourceAccessRequest(
                    resource_name=Job.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=VectorStoreQuery.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=VectorStore.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read',
                ),
                ResourceAccessRequest(
                    resource_name=VectorStoreChunk.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read',
                ),
                ResourceAccessRequest(
                    resource_name=VectorStoreTag.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read',
                )
            ],
            scope=self,
            timeout=Duration.minutes(5),
        )

        self.vacuum = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='vector_vacuum',
            description='Deletes old vector store chunks given an entry',
            entry=self.runtime_path,
            event_type='vector_vacuum_request',
            index='vacuum.py',
            handler='handler',
            function_name=resource_namer('vector-vacuum', scope=self),
            memory_size=1024,
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name=ArchiveEntry.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read',
                ),
                ResourceAccessRequest(
                    resource_name=Job.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=VectorStore.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=VectorStoreChunk.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(10),
        )

        self.vector_store_bucket.grant_read_write(self.vacuum.handler.function)

        self.vector_store_bucket.grant_read_write(self.vector_store_rebalancer.handler.function)

        self.vs_query = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='vs_query',
            description='Handles a query request for a single vector store.',
            entry=self.runtime_path,
            event_type='vs_query',
            index='vs_query.py',
            handler='handler',
            function_name=resource_namer('vs-query', scope=self),
            memory_size=1024,
            managed_policies=[
                ManagedPolicy.from_managed_policy_arn(
                    scope=self,
                    id='vs-query-amazon-bedrock-full-access',
                    managed_policy_arn='arn:aws:iam::aws:policy/AmazonBedrockFullAccess'
                ),
            ],
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_name=Job.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(10),
        )

        self.vector_store_bucket.grant_read(self.vs_query.handler.function)

        self.query_complete = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='query_complete',
            description='Packages the query results and sends them back to the requester.',
            entry=self.runtime_path,
            event_type='query_complete',
            index='query_complete.py',
            handler='handler',
            function_name=resource_namer('query-complete', scope=self),
            memory_size=256,
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_type=ResourceType.TABLE,
                    resource_name=ArchiveEntry.table_name,
                    policy_name='read',
                ),
                ResourceAccessRequest(
                    resource_name=Entry.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read',
                ),
                ResourceAccessRequest(
                    resource_name=Job.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=VectorStoreQuery.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(2),
        )