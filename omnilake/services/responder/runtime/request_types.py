from dataclasses import dataclass
from typing import Dict, List, Optional, Union


@dataclass
class BasicArchiveInformationRequest:
    archive_id: str
    evaluation_type: str # INCLUSIVE or EXCLUSIVE
    max_entries: int
    sample_size_percentage: int
    request_type: str = 'BASIC'


@dataclass
class VectorArchiveInformationRequest:
    archive_id: str
    query_string: str
    evaluation_type: str
    max_entries: int
    sample_size_percentage: Optional[int] = None
    request_type: str = 'VECTOR'


def load_raw_request(request: dict) -> Union[BasicArchiveInformationRequest, VectorArchiveInformationRequest]:
    """
    Load an information request from a dictionary

    Keyword arguments:
    request -- The request
    """
    if request['request_type'] == 'BASIC':
        return BasicArchiveInformationRequest(**request)

    elif request['request_type'] == 'VECTOR':
        return VectorArchiveInformationRequest(**request)

    raise ValueError('Invalid request type')


def load_raw_requests(requests: List[Dict]) -> List[Union[BasicArchiveInformationRequest, VectorArchiveInformationRequest]]:
    """
    Load information requests from a list of dictionaries

    Keyword arguments:
    requests -- The requests
    """
    return [load_raw_request(request) for request in requests]