from os import path

from aws_cdk import (
    RemovalPolicy,
)

from constructs import Construct

from aws_cdk.aws_s3 import Bucket, BucketEncryption

from da_vinci.core.resource_discovery import ResourceType

from da_vinci_cdk.stack import Stack

from da_vinci_cdk.constructs.access_management import ResourceAccessRequest
from da_vinci_cdk.constructs.global_setting import GlobalSetting
from da_vinci_cdk.constructs.service import SimpleRESTService

from omnilake.tables.entries.stack import Entry, EntriesTable
from omnilake.tables.sources.stack import Source, SourcesTable
from omnilake.tables.source_types.stack import SourceType, SourceTypesTable


class LakeRawStorageManagerStack(Stack):
    def __init__(self, app_name: str, app_base_image: str, architecture: str,
                 deployment_id: str, stack_name: str, scope: Construct):
        """
        RAW Storage Manager stack for OmniLake.

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
            requires_exceptions_trap=True,
            required_stacks=[
                EntriesTable,
                SourcesTable,
                SourceTypesTable,
            ],
            deployment_id=deployment_id,
            scope=scope,
            stack_name=stack_name,
        )

        base_dir = self.absolute_dir(__file__)

        self.runtime_path = path.join(base_dir, 'runtime')

        self.raw_entry_bucket = Bucket(
            self,
            'raw_entry_bucket',
            encryption=BucketEncryption.S3_MANAGED,
            removal_policy=RemovalPolicy.DESTROY,
        )

        self.raw_entry_bucket_setting = GlobalSetting(
            namespace='omnilake::storage',
            setting_key='raw_entry_bucket',
            setting_value=self.raw_entry_bucket.bucket_name,
            scope=self,
        )

        self.raw_storage_manager = SimpleRESTService(
            base_image=self.app_base_image,
            description='Manages the raw data storage',
            entry=self.runtime_path,
            index='raw_manager.py',
            handler='handler',
            memory_size=512,
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name=Entry.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=Source.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=SourceType.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read',
                ),
            ],
            scope=self,
            service_name='raw_storage_manager',
        )

        self.raw_entry_bucket.grant_read_write(self.raw_storage_manager.handler.function)