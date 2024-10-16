from datetime import datetime
from dataclasses import asdict, dataclass, field
from typing import Dict, List, Optional, Union


@dataclass
class RequestSuperclass:
    def to_dict(self):
        """
        Return the object as a dictionary.
        """
        return asdict(self)


@dataclass
class AddEntry(RequestSuperclass):
    archive_id: str
    content: str
    sources: List[str]
    effective_on: Optional[str] = None # will be set to time of insertion if not provided
    original: Optional[str] = None
    summarize: Optional[bool] = False


@dataclass
class AddSource(RequestSuperclass):
    source_category: str
    location_id: str
    metadata: Optional[dict] = None
    publication_date: Optional[Union[datetime, str]]  = None

    def __post_init__(self):
        if isinstance(self.publication_date, datetime):
            self.publication_date = self.publication_date.isoformat()


@dataclass
class CreateArchive(RequestSuperclass):
    archive_id: str
    description: str
    storage_type: Optional[str] = 'VECTOR_STORAGE'


@dataclass
class DeleteEntry(RequestSuperclass):
    entry_id: str
    force: Optional[bool] = False


@dataclass
class DeleteSource(RequestSuperclass):
    source_id: str
    force: Optional[bool] = False


@dataclass
class DescribeEntry(RequestSuperclass):
    entry_id: str


@dataclass
class DescribeJob(RequestSuperclass):
    job_id: str
    job_type: str


@dataclass
class DescribeSource(RequestSuperclass):
    source_id: str


@dataclass
class DescribeRequest(RequestSuperclass):
    request_id: str


@dataclass
class GetEntry(RequestSuperclass):
    entry_id: str


@dataclass
class IndexEntry(RequestSuperclass):
    archive_id: str
    entry_id: str


@dataclass
class BasicArchiveInformationRequest:
    archive_id: str
    max_entries: int
    sample_size_percentage: int
    evaluation_type: str = 'INCLUSIVE' # Only one supported type
    request_type: str = 'BASIC'


@dataclass
class VectorArchiveInformationRequest:
    archive_id: str
    evaluation_type: str
    max_entries: int # Must always be set by the requester
    query_string: str = None # required if the evaluation type is EXCLUSIVE
    sample_size_percentage: Optional[int] = None # Must be set if the evaluation type is INCLUSIVE
    request_type: str = 'VECTOR'


@dataclass
class InformationRequest(RequestSuperclass):
    goal: str
    requests: List[Union[Dict, BasicArchiveInformationRequest, VectorArchiveInformationRequest]]
    resource_names: Optional[List[str]] = None

    def __post_init__(self):
        normalized_requests = []

        for req in self.requests:
            if isinstance(req, dict):
                normalized_requests.append(req)

            else:
                normalized_requests.append(asdict(req))

        self.requests = normalized_requests


@dataclass
class UpdateEntry(RequestSuperclass):
    entry_id: str
    content: str