import os 

from aws_cdk import DockerImage, Duration

from constructs import Construct

from da_vinci_cdk.constructs.access_management import (
    ResourceAccessRequest,
)
from da_vinci_cdk.constructs.global_setting import GlobalSetting, GlobalSettingType
from da_vinci_cdk.constructs.service import SimpleRESTService
from da_vinci_cdk.stack import Stack

from omnilake.services.ai_statistics_collector.tables.ai_statistics.stack import (
    AIStatisticsTable,
    InvocationStatistic,
)


class AIStatisticsCollectorStack(Stack):
    def __init__(self, app_base_image: str, app_name: str, architecture: str, deployment_id: str, scope: Construct,
                 stack_name: str, library_base_image: DockerImage):
        """
        Initialize the AIStatisticsTrapStack

        Keyword Arguments:
            app_base_image -- Base image built for the application
            app_name -- Name of the application
            architecture -- Architecture to use for the stack
            deployment_id -- Identifier assigned to the installation
            scope -- Parent construct for the stack
            stack_name -- Name of the stack
            library_base_image -- Base image built for the library
        """

        base_dir = self.absolute_dir(__file__)

        self.runtime_path = os.path.join(base_dir, 'runtime')

        super().__init__(
            app_base_image=app_base_image,
            app_name=app_name,
            architecture=architecture,
            deployment_id=deployment_id,
            scope=scope,
            stack_name=stack_name,
            library_base_image=library_base_image,
            requires_exceptions_trap=True,
            required_stacks=[
                AIStatisticsTable,
            ],
        )

        self.trap = SimpleRESTService(
            base_image=self.app_base_image,
            architecture=self.architecture,
            description='Service to trap AI statistics',
            entry=self.runtime_path,
            handler='api',
            index='api.py',
            resource_access_requests=[
                ResourceAccessRequest(
                    resource_name=InvocationStatistic.table_name,
                    resource_type='table',
                    policy_name='read_write',
                ),   
            ],
            scope=self,
            service_name='ai_statistics_collector',
            timeout=Duration.seconds(30),
        )

        self.ttl_setting = GlobalSetting(
            description='The total number of days for statistics to be retained, this is used to calculate the time to live. CHANGES WILL NOT AFFECT EXISTING ENTRIES!!',
            namespace='omnilake::ai_statistics_collector',
            setting_key='statistic_retention_days',
            setting_type=GlobalSettingType.INTEGER,
            setting_value=90,
            scope=self,
        )