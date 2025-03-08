"""
Handles the lookup of data in a Web Site archive.
"""
import logging

import aiohttp
import asyncio

from datetime import datetime, UTC as utc_tz
from urllib.parse import urljoin
from typing import Dict, List

from bs4 import BeautifulSoup

from markdownify import markdownify as md

from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from da_vinci.exception_trap.client import ExceptionReporter

from omnilake.internal_lib.event_definitions import (
    LakeRequestLookupResponse,
    LakeRequestInternalRequestEventBodySchema,
)

from omnilake.internal_lib.clients import RawStorageManager

from omnilake.tables.provisioned_archives.client import ArchivesClient
from omnilake.tables.jobs.client import JobsClient


REQUEST_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:98.0) Gecko/20100101 Firefox/98.0",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Accept-Encoding": "gzip, deflate",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
    "Cache-Control": "max-age=0",
}


async def load_web_content(urls: List[str]) -> List[str]:
    """
    Asynchronously loads the web content from a list of URLs and return the resulting list of entry ids.

    Keyword Arguments:
        urls: A list of URLs to load
    """
    raw_storage = RawStorageManager()

    loaded_content = []

    for url in urls:
        async with aiohttp.ClientSession() as session:
            async with session.get(url=url, headers=REQUEST_HEADERS) as response:
                if response.status != 200:
                    logging.warning(f"Failed to retrieve {url}: {response.status}")

                    continue

                html = await response.text()

                soup = BeautifulSoup(html, 'html.parser')

                for data in soup(['style', 'script']):
                    # Remove tags
                    data.decompose()

                body = soup.find('body')

                formatted_body = md(html=str(body))

                if not formatted_body:
                    logging.warning(f"Failed to format {url}")

                    continue

                resp = raw_storage.create_entry_with_source(
                    content=formatted_body,
                    effective_on=datetime.now(tz=utc_tz),
                    source_type="web_page_content",
                    source_arguments={"url": url},
                )

                entry_id = resp.response_body["entry_id"]

                loaded_content.append(entry_id)

    return loaded_content


_FN_NAME = "omnilake.constructs.archives.web_site.lookup" 


@fn_event_response(exception_reporter=ExceptionReporter(), function_name=_FN_NAME, logger=Logger(_FN_NAME),
                   handle_callbacks=True)
def handler(event: Dict, context: Dict):
    '''
    Handles the lookup of data in a web site archive.
    '''
    logging.debug(f'Received request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=LakeRequestInternalRequestEventBodySchema,
    )

    # Prep for the child job
    parent_job_id = event_body["parent_job_id"]

    parent_job_type = event_body["parent_job_type"]

    jobs_client = JobsClient()

    parent_job = jobs_client.get(job_type=parent_job_type, job_id=parent_job_id, consistent_read=True)

    child_job = parent_job.create_child(job_type="ARCHIVE_WEB_SITE_LOOKUP")

    # Execute the entry lookup under the child job
    with jobs_client.job_execution(child_job):
        retrieval_instructions = event_body.get("request_body")

        archive_id = retrieval_instructions.get("archive_id")

        # Load the archive from the table to retrieve the base URL
        archives_client = ArchivesClient()

        archive = archives_client.get(archive_id=archive_id)

        if not archive:
            raise Exception(f"Archive {archive_id} not found")

        # Grab the paths from the lookup config
        retrieve_paths = retrieval_instructions["retrieve_paths"]

        # Generate URLs using URL Join
        urls = [str(urljoin(archive.configuration["base_url"], path)) for path in retrieve_paths]

        retrieved_entries = asyncio.run(load_web_content(urls))

        lake_request_id = event_body.get("lake_request_id")

        response_obj = ObjectBody(
            body={
                "entry_ids": retrieved_entries,
                "lake_request_id": lake_request_id,
            },
            schema=LakeRequestLookupResponse,
        )

        event_publisher = EventPublisher()

        event_publisher.submit(
            event=EventBusEvent(
                body=response_obj.to_dict(ignore_unkown=True),
                event_type=response_obj.get("event_type", strict=True),
            ),
        )