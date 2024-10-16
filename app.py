import os

from da_vinci_cdk.application import Application
from da_vinci_cdk.stack import Stack

from omnilake.api.stack import OmniLakeAPIStack

from omnilake.services.ingestion.stack import IngestionServiceStack
from omnilake.services.responder.stack import ResponderEngineStack
from omnilake.services.storage.stack import StorageManagerStack

base_dir = Stack.absolute_dir(__file__)

deployment_id = os.getenv('OMNILAKE_DEPLOYMENT_ID', 'dev')

omnilake = Application(
    app_entry=base_dir,
    app_name='omnilake',
    create_hosted_zone=False,
    deployment_id=deployment_id,
    disable_docker_image_cache=True,
    include_event_bus=True,
    log_level='DEBUG',
    s3_logging_bucket_name_prefix='caylent-rnd-',
    s3_logging_bucket_object_retention_days=30,
)

omnilake.add_uninitialized_stack(OmniLakeAPIStack)

omnilake.add_uninitialized_stack(IngestionServiceStack)

omnilake.add_uninitialized_stack(ResponderEngineStack)

omnilake.add_uninitialized_stack(StorageManagerStack)

omnilake.synth()