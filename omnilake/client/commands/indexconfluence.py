import time

from logging import getLogger
from typing import Optional

from atlassian import Confluence
import html2text

from omnilake.client.client import OmniLake
from omnilake.client.request_definitions import (
    AddEntry,
    AddSource,
    CreateSourceType,
    CreateArchive,
    VectorArchiveConfiguration,
)

from omnilake.client.commands.base import Command

logger = getLogger(__name__)


class IndexConfluenceSpaceCommand(Command):
    command_name='index-confluence-space'

    description='Index a confluence space as vector store'

    def __init__(self, omnilake_app_name: Optional[str] = None, omnilake_deployment_id: Optional[str] = None):
        super().__init__()

        # Initialize the OmniLake client
        self.omnilake = OmniLake(
            app_name=omnilake_app_name,
            deployment_id=omnilake_deployment_id,
        )
        self.confluence = None

    @classmethod
    def configure_parser(cls, parser):
        index_parser = parser.add_parser('index-confluence-space', help='Indexes an entire confluence space as vector store')
        index_parser.add_argument('url',help='URL of the confluence space')
        index_parser.add_argument('username',help='Username of the confluence space')
        index_parser.add_argument('apikey',help='Api Key of the confluence space')
        index_parser.add_argument('spacekey',help='Space Key of the confluence space')
        index_parser.add_argument('archiveid',help='Id for the archive')
        return index_parser

    def create_archive(self, archive_name: str):
        """
        Create an archive if it doesn't exist
        """
        try:
            archive = CreateArchive(
                archive_id=archive_name,
                configuration=VectorArchiveConfiguration(tag_model_id="anthropic.claude-v2:1"),
                description=f'Archive for confluence space {archive_name} ',
            )

            self.omnilake.request(archive)

            print('Provisioning archive...')

            time.sleep(30)
        except Exception as e:
            if "Archive already exists" in str(e):
                print('Archive already exists')

            else:
                raise

    def list_pages(self,space_key):
        """
        List all existing pages in the Confluence space.
        :par fram space_key: Confluence space key to list pagesom
        :return: List of pages
        """
        print('Listing pages for space {}'.format(space_key))
        return self.confluence.get_all_pages_from_space(space_key, start=0, limit=0)

    def create_confluence_page_source_type(self):
        """
        Create a source type if it doesn't exist
        """
        try:
            source_type = CreateSourceType(
                name='confluence-page',
                description='A page uploaded from a confluence space',
                required_fields=['page_id'],
            )

            self.omnilake.request(source_type)

            print('Source type "confluence-page" created')
        except Exception as e:
            if "Source type already exists" in str(e):
                print('Source type "local_file" already exists')
            else:
                raise

    def _index_confluence_page(self, archive_id: str, page_id: str):
        """
        Download and save the content of a Confluence page
        :param archive_id: the id of the archive where the page will be assigned to
        :param page_id: the id of the confluence page that is being indexed
        """
        print(f'Indexing page {page_id}...')
        # Retrieve page content in storage format (HTML)
        page_content = self.confluence.get_page_by_id(page_id, expand='body.storage')

        html_content = page_content['body']['storage']['value']
        clear_html_content = html2text.html2text(html_content)

        # Placeholder for future implementation
        source = AddSource(
            source_type="confluence-page",
            source_arguments={
                "page_id": page_id
            })
        source_result = self.omnilake.request(source)

        source_rn = source_result.response_body["resource_name"]

        entry = AddEntry(
            content=clear_html_content,
            sources=[source_rn],
            destination_archive_id=archive_id,
            original_of_source=source_rn
        )

        self.omnilake.request(entry)
        print('Page indexed successfully')


    def run(self, args):
        print('Indexing a confluence space...')
        print(f'Configurations: {args}')
        self.confluence = Confluence(
            url=args.url,
            username=args.username,
            password=args.apikey
        )
        pages = self.list_pages(args.spacekey)

        self.create_confluence_page_source_type()
        self.create_archive(args.archiveid)

        for page in pages:
            self._index_confluence_page(archive_name=args.archiveid, page_id=page['id'])

        print('Finished indexing confluence space')

