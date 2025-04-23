import os
import aws_cdk

from da_vinci_cdk.application import Application, ResourceDiscoveryStorageSolution
from da_vinci_cdk.stack import Stack

from omnilake.api.stack import OmniLakeAPIStack

base_dir = Stack.absolute_dir(__file__)

deployment_id = os.getenv('OMNILAKE_DEPLOYMENT_ID', 'dev')

omnilake = Application(
    app_entry=base_dir,
    app_name='omnilake',
    create_hosted_zone=False,
    deployment_id=deployment_id,
    disable_docker_image_cache=True,
    enable_exception_trap=True,
    enable_event_bus=True,
    log_level='DEBUG',
    resource_discovery_storage_solution=ResourceDiscoveryStorageSolution.DYNAMODB,
    #architecture=aws_cdk.aws_lambda.Architecture.X86_64, # Uncomment if you want to use x86_64 architecture
)

omnilake.add_uninitialized_stack(OmniLakeAPIStack)

omnilake.synth()