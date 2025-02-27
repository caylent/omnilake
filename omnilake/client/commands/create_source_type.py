#create source type
import time

from logging import getLogger
from typing import Optional
import json

import pypdf

from omnilake.client.client import OmniLake
from omnilake.client.request_definitions import (
    AddEntry,
    AddSource,
    CreateSourceType,
    CreateArchive,
    VectorArchiveConfiguration,
)

from omnilake.client.commands.base import Command

from omnilake.client.fileutil import collect_files


logger = getLogger(__name__)


class CreateSourceTypeCommand(Command):
    command_name='create-source-type'

    description='Create a new source type'

    def __init__(self, omnilake_app_name: Optional[str] = None, omnilake_deployment_id: Optional[str] = None):
        super().__init__()

        # Initialize the OmniLake client
        self.omnilake = OmniLake(
            app_name=omnilake_app_name,
            deployment_id=omnilake_deployment_id,
        )

    @classmethod
    def configure_parser(cls, parser):
        index_parser = parser.add_parser('create-source-type', help='Indexes the source code')
        index_parser.add_argument('--name', help='Name of the new source type')
        index_parser.add_argument('--description', help='Version of the new source type')
        index_parser.add_argument('--required-fields', help='Array of required fields')
        return index_parser

    def create_source_type(self, name, description, required_fields):
        """
        Create a source type if it doesn't exist
        """
        try:
            source_type = CreateSourceType(
                name=name,
                description=description,
                required_fields=required_fields,
            )

            self.omnilake.request(source_type)

            print(f'Source type "{name}" created')
        except Exception as e:
            if "Source type already exists" in str(e):
                print(f'Source type "{name}" already exists')

            else:
                raise

    def run(self, args):
        print('Creating source type...')
        name = args.name
        description = args.description
        required_fields = args.required_fields.split(',')
        # Create the source type if it doesn't exist
        self.create_source_type(name, description, required_fields)
