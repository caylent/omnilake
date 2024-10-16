from os import path

from aws_cdk import (
    Duration,
    RemovalPolicy,
)

from constructs import Construct

from aws_cdk.aws_iam import ManagedPolicy
from aws_cdk.aws_s3 import Bucket, BucketEncryption

from da_vinci.core.resource_discovery import ResourceType

from da_vinci_cdk.stack import Stack

from da_vinci_cdk.constructs.access_management import ResourceAccessRequest
from da_vinci_cdk.constructs.base import resource_namer
from da_vinci_cdk.constructs.global_setting import GlobalSetting
from da_vinci_cdk.constructs.event_bus import EventBusSubscriptionFunction
from da_vinci_cdk.constructs.service import SimpleRESTService

from omnilake.tables.entries.stack import Entry, EntriesTable
from omnilake.tables.information_requests.stack import InformationRequest, InformationRequestsTable
from omnilake.tables.jobs.stack import Job, JobsTable
from omnilake.tables.sources.stack import Source, SourcesTable


class ReaperStack(Stack):
    def __init__(self, app_name: str, app_base_image: str, architecture: str,
                 deployment_id: str, stack_name: str, scope: Construct):
        """
        Service that handles deletion of sources and entries.

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
                EntriesTable,
                InformationRequestsTable,
                JobsTable,
                SourcesTable,
            ],
            deployment_id=deployment_id,
            scope=scope,
            stack_name=stack_name,
        )

        base_dir = self.absolute_dir(__file__)

        self.runtime_path = path.join(base_dir, 'runtime')

        self.entry_reaper = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='entry_reaper',
            description='Reaper for entries',
            entry=self.runtime_path,
            event_type='reap_entry',
            index='entry.py',
            handler='handler',
            function_name=resource_namer('entry-reaper', scope=self),
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name='event_bus',
                    resource_type=ResourceType.ASYNC_SERVICE,
                ),
                ResourceAccessRequest(
                    resource_name=Job.table_name,
                    resource_type=ResourceType.TABLE,
                ),
                ResourceAccessRequest(
                    resource_name=VectorStore.table_name,
                    resource_type=ResourceType.TABLE,
                ),
            ],
            scope=self,
            timeout=Duration.minutes(10),
        )

        self.source_reaper = EventBusSubscriptionFunction(
        )