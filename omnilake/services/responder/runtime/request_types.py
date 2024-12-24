from dataclasses import dataclass
from typing import Dict, List, Optional, Union


@dataclass
class BasicInformationRetrievalRequest:
    archive_id: str
    max_entries: int
    prioritize_tags: Optional[List[str]] = None
    request_type: str = 'BASIC'


@dataclass
class RelatedInformationRetrievalRequest:
    related_request_id: str
    request_type: str = 'RELATED'


@dataclass
class VectorInformationRetrievalRequest:
    archive_id: str
    query_string: str
    max_entries: int
    prioritize_tags: Optional[List[str]] = None
    request_type: str = 'VECTOR'


def load_raw_request(request: dict) -> Union[BasicInformationRetrievalRequest, RelatedInformationRetrievalRequest, VectorInformationRetrievalRequest]:
    """
    Load an information request from a dictionary

    Keyword arguments:
    request -- The request
    """
    if request['request_type'] == 'BASIC':
        return BasicInformationRetrievalRequest(**request)

    elif request['request_type'] == 'RELATED':
        return RelatedInformationRetrievalRequest(**request)

    elif request['request_type'] == 'VECTOR':
        return VectorInformationRetrievalRequest(**request)

    raise ValueError('Invalid request type')


def load_raw_requests(requests: List[Dict]) -> List[Union[BasicInformationRetrievalRequest, RelatedInformationRetrievalRequest, VectorInformationRetrievalRequest]]:
    """
    Load information requests from a list of dictionaries

    Keyword arguments:
    requests -- The requests
    """
    return [load_raw_request(request) for request in requests]