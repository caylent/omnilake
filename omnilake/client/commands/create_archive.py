#create archive
import time

from logging import getLogger
from typing import Optional

from omnilake.client.client import OmniLake
from omnilake.client.construct_request_definitions import WebSiteArchiveConfiguration, BasicArchiveConfiguration
from omnilake.client.request_definitions import (
    CreateArchive,
    VectorArchiveConfiguration,
)

from omnilake.client.commands.base import Command

logger = getLogger(__name__)


class CreateArchiveCommand(Command):
    command_name='create-archive'

    description='Create a new archive'

    def __init__(self, omnilake_app_name: Optional[str] = None, omnilake_deployment_id: Optional[str] = None):
        super().__init__()

        # Initialize the OmniLake client
        self.omnilake = OmniLake(
            app_name=omnilake_app_name,
            deployment_id=omnilake_deployment_id,
        )

    def parse_key_value_pairs(self,pairs):
        params_dict = {}
        for pair in pairs:
            key, value = pair.split('=')
            params_dict[key] = value
        return params_dict

    @classmethod
    def configure_parser(cls, parser):
        index_parser = parser.add_parser('create-archive', help='Creates a new archive')
        index_parser.add_argument('--name', help='Name of the new archive')
        index_parser.add_argument('--description', help='Description of the new archive')
        index_parser.add_argument('--configuration-type', help='Type of configuration. Ex: VectorStoreConfiguration')
        index_parser.add_argument('--configuration-params', nargs="*", help='Configuration parameters')
        return index_parser

    def get_configuration_type(self, configuration_type: str, configuration_params: dict):
        if(configuration_type=='VectorArchiveConfiguration'):
            return VectorArchiveConfiguration(
                tag_model_id=configuration_params.get('tag_model_id',None),
                chunk_body_overlap_percentage=configuration_params.get('chunk_body_overlap_percentage'),
                max_chunk_length=configuration_params.get('max_chunk_length'),
                retain_latest_originals_only=configuration_params.get('retain_latest_originals_only'),
                tag_hint_instructions=configuration_params.get('tag_hint_instructions'))

        if(configuration_type=='WebSiteArchiveConfiguration'):
            return WebSiteArchiveConfiguration(
                base_url=configuration_params.get("base_url"),
                test_path=configuration_params.get("test_path"))

        if(configuration_type=='BasicArchiveConfiguration'):
            return BasicArchiveConfiguration()



    def create_archive(self, archive_name: str, description:str, configuration_type: str, configuration_params: dict):
        """
        Create an archive if it doesn't exist
        """
        try:
            archive = CreateArchive(
                archive_id=archive_name,
                configuration=self.get_configuration_type(configuration_type, configuration_params),
                description=description
            )

            self.omnilake.request(archive)
            print('Provisioning archive...')
            time.sleep(30)
            print(f'archive created.')
        except Exception as e:
            if "Archive already exists" in str(e):
                print('Archive already exists')
            else:
                raise

    def run(self, args):
        print('Creating source type...')
        name = args.name
        configuration_type = args.configuration_type
        print(args.configuration_params)
        configuration_params = self.parse_key_value_pairs(args.configuration_params)
        description = args.description
        # Create the archive if it doesn't exist
        self.create_archive(name, description, configuration_type, configuration_params)

