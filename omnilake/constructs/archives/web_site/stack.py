from os import path

from aws_cdk import Duration

from constructs import Construct

from da_vinci.core.resource_discovery import ResourceType

from da_vinci_cdk.stack import Stack

from da_vinci_cdk.constructs.access_management import ResourceAccessRequest
from da_vinci_cdk.constructs.base import resource_namer
from da_vinci_cdk.constructs.event_bus import EventBusSubscriptionFunction

from da_vinci_cdk.framework_stacks.services.event_bus.stack import EventBusStack

from omnilake.tables.jobs.stack import Job, JobsTable
from omnilake.tables.provisioned_archives.stack import Archive, ProvisionedArchivesTable
from omnilake.tables.source_types.stack import SourceType, SourceTypesTable

from omnilake.tables.registered_request_constructs.cdk import (
    ArchiveConstructSchemas,
    RegisteredRequestConstruct, 
    RegisteredRequestConstructObj,
    RequestConstructType,
)

from omnilake.services.raw_storage_manager.stack import LakeRawStorageManagerStack

from omnilake.tables.registered_request_constructs.stack import RegisteredRequestConstructsTable

from omnilake.constructs.archives.web_site.schemas import (
    WebSiteArchiveLookupObjectSchema,
    WebSiteArchiveProvisionObjectSchema,
)


class LakeConstructArchiveWebSiteStack(Stack):
    def __init__(self, app_name: str, app_base_image: str, architecture: str,
                 deployment_id: str, stack_name: str, scope: Construct):
        """
        Web Site Archive stack for OmniLake.

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
                EventBusStack,
                JobsTable,
                LakeRawStorageManagerStack,
                ProvisionedArchivesTable,
                RegisteredRequestConstructsTable,
                SourceTypesTable,
            ],
            deployment_id=deployment_id,
            scope=scope,
            stack_name=stack_name,
        )

        base_dir = self.absolute_dir(__file__)

        self.runtime_path = path.join(base_dir, 'runtime')

        schemas = ArchiveConstructSchemas(
            lookup=WebSiteArchiveLookupObjectSchema,
            provision=WebSiteArchiveProvisionObjectSchema,
        )

        self.registered_request_construct_obj = RegisteredRequestConstructObj(
            registered_construct_type=RequestConstructType.ARCHIVE,
            registered_type_name='WEB_SITE',
            description='Enables direct access to a particular web site\'s data.',
            schemas=schemas,
        )

        self.archive_provisioner = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='web_site_archive_provisioner',
            description='Provisions Web Site archives.',
            entry=self.runtime_path,
            event_type=self.registered_request_construct_obj.get_operation_event_name('provision'),
            index='provisioner.py',
            handler='handler',
            function_name=resource_namer('web-site-archive-provisioner', scope=self),
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
                    resource_name=SourceType.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(1),
        )

        self.lookup = EventBusSubscriptionFunction(
            base_image=self.app_base_image,
            construct_id='web_site_archive_data_retrieval',
            description='Retrieves pages from a web site based on the provisioned archive.',
            entry=self.runtime_path,
            event_type=self.registered_request_construct_obj.get_operation_event_name('lookup'),
            index='lookup.py',
            handler='handler',
            function_name=resource_namer('web-site-archive-data-retrieval', scope=self),
            memory_size=512,
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
                    policy_name='read',
                ),
                ResourceAccessRequest(
                    resource_name=Job.table_name,
                    resource_type=ResourceType.TABLE,
                    policy_name='read_write',
                ),
            ],
            scope=self,
            timeout=Duration.minutes(3),
        )

        # Register the Basic Archive Construct
        RegisteredRequestConstruct.from_definition(registered_construct=self.registered_request_construct_obj, scope=self)
