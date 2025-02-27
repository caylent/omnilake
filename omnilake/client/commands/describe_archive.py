import time
from logging import getLogger
from typing import Optional
from omnilake.client.client import OmniLake
from omnilake.client.request_definitions import DescribeArchive
from omnilake.client.commands.base import Command

logger = getLogger(__name__)

class DescribeArchiveCommand(Command):
    command_name='describe-archive'

    description='Describe an archive'

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
        index_parser = parser.add_parser('describe-archive', help='Describes an archive')
        index_parser.add_argument('name', help='Name of the archive')
        return index_parser

    def describe_archive(self, archive_name: str):
        """
        Describe an archive
        """
        try:
            archive = DescribeArchive(archive_name)
            response = self.omnilake.request(archive)
            response_body = response.response_body
            print(f'ArchiveId:{response_body["archive_id"]}')
            print(f'Description:{response_body["description"]}')
            print(f'Status:{response_body["status"]}')
            print(f'Archive Type:{response_body["archive_type"]}')
            print(f'Configuration Parameters:{response_body["configuration"]}')
            time.sleep(30)

        except Exception as e:
            print('Error describing archive:', e)
            raise

    def run(self, args):
        print('Describing archive...')
        name = args.name
        # Describe an archive
        self.describe_archive(name)
