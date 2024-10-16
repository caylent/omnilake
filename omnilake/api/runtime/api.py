'''
Primary API handler for the OmniLake API
'''
import json
import logging

from typing import Dict

from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import fn_exception_reporter

from omnilake.api.runtime.archive import ArchiveAPI
from omnilake.api.runtime.construct import ParentAPI
from omnilake.api.runtime.entry import EntriesAPI
from omnilake.api.runtime.information import InformationRequestAPI
from omnilake.api.runtime.job import JobsAPI
from omnilake.api.runtime.source import SourcesAPI


@fn_exception_reporter('omnilake_api', logger=Logger('omnilake.api.public'))
def handler(event: Dict, context: Dict) -> Dict:
    """
    Lambda handler for the Main OmniLake API

    Keyword Arguments:
    event -- The event data
    context -- The context data

    Returns:
    dict -- The response
    """
    logging.debug(f'Recieved request: {event}')

    parent_api = ParentAPI(
        child_apis=[
            ArchiveAPI,
            EntriesAPI,
            InformationRequestAPI,
            JobsAPI,
            SourcesAPI,
        ],
    )

    body = event.get('body')

    kwargs = {}

    if body:
        kwargs = json.loads(body)

    return parent_api.execute_path(path=event['rawPath'], **kwargs)