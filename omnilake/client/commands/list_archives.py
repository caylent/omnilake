import time

from logging import getLogger
from typing import Optional
import json

import pypdf

from omnilake.client.client import OmniLake
from omnilake.client.construct_request_definitions import WebSiteArchiveConfiguration, BasicArchiveConfiguration
from omnilake.client.request_definitions import (
    AddEntry,
    AddSource,
    CreateSourceType,
    ListProvisionedArchives,
    VectorArchiveConfiguration,
)

from omnilake.client.commands.base import Command

from omnilake.client.fileutil import collect_files


logger = getLogger(__name__)


class ListArchivesCommand(Command):
    command_name='list-archives'

    description='List existing provisioned archives'

    def __init__(self, omnilake_app_name: Optional[str] = None, omnilake_deployment_id: Optional[str] = None):
        super().__init__()

        # Initialize the OmniLake client
        self.omnilake = OmniLake(
            app_name=omnilake_app_name,
            deployment_id=omnilake_deployment_id,
        )


    @classmethod
    def configure_parser(cls, parser):
        index_parser = parser.add_parser('list-archives', help='Lists existing provisioned archives')

        return index_parser

    def list_archives(self):
        """
        Create an archive if it doesn't exist
        """
        try:
            archive = ListProvisionedArchives()

            archives = self.omnilake.request(archive)
            print(archives)

        except Exception as e:
            if "Archive already exists" in str(e):
                print('Archive already exists')
            else:
                raise

    def run(self, args):
        print('Listing archives...')
        self.list_archives()

