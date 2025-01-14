import time

from logging import getLogger
from typing import Optional

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


class RefreshIndexCommand(Command):
    command_name='index'

    description='Create or update the index'

    def __init__(self, omnilake_app_name: Optional[str] = None, omnilake_deployment_id: Optional[str] = None):
        super().__init__()

        # Initialize the OmniLake client
        self.omnilake = OmniLake(
            app_name=omnilake_app_name,
            deployment_id=omnilake_deployment_id,
        )

    @classmethod
    def configure_parser(cls, parser):
        index_parser = parser.add_parser('index', help='Indexes the source code')

        return index_parser

    def create_archive(self, archive_name: str):
        """
        Create an archive if it doesn't exist
        """
        try:
            archive = CreateArchive(
                archive_id=archive_name,
                configuration=VectorArchiveConfiguration(),
                description=f'Archive for local file directory {archive_name} from somone\'s computer :shrug:',
            )

            self.omnilake.request(archive)

            time.sleep(30)
        except Exception as e:
            if "Archive already exists" in str(e):
                print('Archive already exists')

            else:
                raise

    def create_source_type(self):
        """
        Create a source type if it doesn't exist
        """
        try:
            source_type = CreateSourceType(
                name='local_file',
                description='A file uploaded from a local system',
                required_fields=['file_name', 'full_file_path', 'file_extension'],
            )

            self.omnilake.request(source_type)

            print('Source type "local_file" created')
        except Exception as e:
            if "Source type already exists" in str(e):
                print('Source type "local_file" already exists')

            else:
                raise

    def _index_file(self, archive_name: str, file_contents: str, file_name: str, file_path: str,
                    page_number: Optional[int] = None):
        """
        Index a file

        Keyword arguments:
        archive_name -- the name of the archive
        file_contents -- the contents of the file
        file_name -- the name of the file
        file_path -- the path of the file
        page_number -- the page number of the file (default None)
        """
        full_file_name = file_name

        if page_number:
            full_file_name = f'{file_name}.{page_number}'

        source = AddSource(
            source_type='local_file',
            source_arguments={
                'file_name': full_file_name,
                'file_extension': file_name.split('.')[-1],
                'full_file_path': file_path,
            },
        )

        source_result = self.omnilake.request(source)

        source_rn = source_result.response_body['resource_name']

        entry = AddEntry(
            content=file_contents,
            sources=[source_rn],
            destination_archive_id=archive_name,
            original_of_source=source_rn,
        )

        self.omnilake.request(entry)

    def run(self, args):
        print('Indexing...')

        starting_dir = args.base_dir

        logger.debug(f'Indexing base directory: {starting_dir}')

        archive_name = starting_dir.split('/')[-1]

        # Create the archive if it doesn't exist
        # archive should enforce latest version
        self.create_archive(archive_name)

        # Create the source type if it doesn't exist
        self.create_source_type()

        # Iterate over the files in the base directory and load them into the archive
        collected_files = collect_files(starting_dir, ignore_patterns=['.git*', '*__pycache__*', '*.pyc', 'poetry.lock', 'cdk.out*', '.DS_Store'])

        for collected_file in collected_files:
            relative_to_base = str(collected_file.relative_to(starting_dir))

            file_contents = collected_file.read_bytes()

            if collected_file.name.endswith('.pdf'):
                print('Detected PDF file, extracting text...')

                pdf_reader = pypdf.PdfReader(stream=collected_file)

                print('Splitting PDF into pages...')

                for page_number, page in enumerate(pdf_reader.pages):
                    self._index_file(
                        archive_name=archive_name,
                        file_contents=page.extract_text(),
                        file_name=collected_file.name,
                        file_path=relative_to_base,
                        page_number=page_number,
                    )

                    print(f'Added {relative_to_base} page {page_number}')

                continue

            decoded_contents = file_contents.decode(encoding='utf-8', errors='ignore')

            self._index_file(
                archive_name=archive_name,
                file_contents=decoded_contents,
                file_name=collected_file.name,
                file_path=relative_to_base,
            )

            print(f'Added {relative_to_base}')

        print('Indexing complete')