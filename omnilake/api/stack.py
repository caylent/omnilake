from os import path

from aws_cdk import (
    Duration,
)

from constructs import Construct

from da_vinci.core.resource_discovery import ResourceType

from da_vinci_cdk.stack import Stack

from da_vinci_cdk.constructs.access_management import ResourceAccessRequest
from da_vinci_cdk.constructs.service import SimpleRESTService

from omnilake.tables.archives.stack import Archive, ArchiveTable
from omnilake.tables.entries.stack import Entry, EntriesTable
from omnilake.tables.information_requests.stack import (
    InformationRequest,
    InformationRequestsTable,
)
from omnilake.tables.jobs.stack import Job, JobsTable
from omnilake.tables.sources.stack import Source, SourcesTable
from omnilake.tables.source_types.stack import SourceType, SourceTypesTable

from omnilake.services.ingestion.stack import IngestionServiceStack
from omnilake.services.responder.stack import ResponderEngineStack
from omnilake.services.storage.basic.stack import BasicArchiveManagerStack
from omnilake.services.storage.raw.stack import RawStorageManagerStack
from omnilake.services.storage.vector.stack import VectorArchiveManagerStack



class OmniLakeAPIStack(Stack):
    def __init__(self, app_name: str, app_base_image: str, architecture: str,
                 deployment_id: str, stack_name: str, scope: Construct):
        """
        OmniLake Public API Service

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
                BasicArchiveManagerStack,
                EntriesTable,
                InformationRequestsTable,
                IngestionServiceStack,
                JobsTable,
                RawStorageManagerStack,
                ResponderEngineStack,
                SourcesTable,
                SourceTypesTable,
                VectorArchiveManagerStack,
            ],
            deployment_id=deployment_id,
            scope=scope,
            stack_name=stack_name,
        )

        base_dir = self.absolute_dir(__file__)

        self.runtime_path = path.join(base_dir, 'runtime')

        self.api_handler = SimpleRESTService(
            base_image=self.app_base_image,
            entry=self.runtime_path,
            index='api',
            handler='handler',
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
                    resource_name=Archive.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=Entry.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read',
                ),
                ResourceAccessRequest(
                    resource_name=InformationRequest.table_name,
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
                    policy_name='read_write',
                ),
                ResourceAccessRequest(
                    resource_name=SourceType.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
            ],
            scope=self,
            service_name='omnilake-public-api',
            timeout=Duration.minutes(5),
        )