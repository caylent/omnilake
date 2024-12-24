
from dataclasses import asdict, dataclass
from typing import Dict, List, Optional, Union

from omnilake.internal_lib.job_types import JobType


@dataclass
class GenericEventBody:
    def to_dict(self):
        return asdict(self)


@dataclass
class AddEntryBody(GenericEventBody):
    content: str
    sources: List[str]
    job_id: str
    archive_id: str = None
    effective_on: str = None
    original_source: str = None 
    summarize: bool = False
    title: str = None
    job_type: str = JobType.ADD_ENTRY
    event_type: str = 'add_entry'


@dataclass
class CreateBasicArchiveBody(GenericEventBody):
    archive_id: str
    description: str
    job_id: str
    retain_latest_originals_only: bool = True
    storage_type: str = 'BASIC'
    tag_hint_instructions: str = None
    visibility: str = 'PUBLIC'
    job_type: str = JobType.CREATE_ARCHIVE
    event_type: str = 'create_basic_archive'


@dataclass
class CreateVectorArchiveBody(GenericEventBody):
    archive_id: str
    description: str
    job_id: str
    retain_latest_originals_only: bool = True
    storage_type: str = 'VECTOR'
    tag_hint_instructions: str = None
    visibility: str = 'PUBLIC'
    job_type: str = JobType.CREATE_ARCHIVE
    event_type: str = 'create_vector_archive'


@dataclass
class GenerateEntryTagsBody(GenericEventBody):
    """
    The body of the generate_entry_tags event.
    """
    archive_id: str
    entry_id: str
    content: str
    parent_job_id: str
    parent_job_type: Union[str, JobType]
    callback_body: Dict = None
    event_type: str = 'generate_entry_tags'


@dataclass
class IndexBasicEntryBody(GenericEventBody):
    archive_id: str
    entry_id: str
    job_id: str = None
    job_type: str = JobType.INDEX_ENTRY
    effective_on: str = None
    original_of_source: str = None
    event_type: str = 'index_basic_entry'


@dataclass
class IndexVectorEntryBody(GenericEventBody):
    archive_id: str
    entry_id: str
    job_id: str = None
    job_type: str = JobType.INDEX_ENTRY
    effective_on: str = None
    event_type: str = 'index_vector_entry'
    original_of_source: str = None
    vector_store_id: str = None


@dataclass
class InformationRequestBody(GenericEventBody):
    request_id: str
    retrieval_requests: List[Dict] = None # This is also allowed because of the below reason lol
    goal: str = None # This is allowed b/c of the hack ass way I re-use this event body
    job_id: str = None
    job_type: str = JobType.INFORMATION_REQUEST
    request_stage: str = 'INITIAL'
    resource_names: List[str] = None
    responder_model_id: Optional[str] = None
    responder_prompt: Optional[str] = None
    responder_model_params: Optional[Dict] = None
    summarization_algorithm: Optional[str] = None
    summarization_prompt: Optional[str] = None
    summarization_model_id: Optional[str] = None
    summarization_model_params: Optional[Dict] = None
    event_type: str = 'start_information_request'


@dataclass
class QueryRequestBody(GenericEventBody):
    archive_id: str
    max_entries: int
    request_id: str
    query_string: str
    event_type: str = 'query_request'
    parent_job_id: str = None
    parent_job_type: str = None
    prioritize_tags: List[str] = None


@dataclass
class QueryCompleteBody(GenericEventBody):
    query_id: str
    resource_names: List[str] = None
    event_type: str = 'query_complete'


@dataclass
class ReapEntryBody(GenericEventBody):
    archive_id: str
    entry_id: str
    job_id: str
    job_type: str = JobType.DELETE_ENTRY
    force: bool = False
    event_type: str = 'reap_entry'


@dataclass
class ReapSourceBody(GenericEventBody):
    archive_id: str
    source_id: str
    source_type: str
    job_id: str
    job_type: str = JobType.DELETE_SOURCE
    force: bool = False
    event_type: str = 'reap_source'


@dataclass
class SaveEntryBody(GenericEventBody):
    archive_id: str
    content: str
    entry_id: str
    job_id: str
    job_type: str = JobType.ADD_ENTRY # This is the parent job type
    event_type: str = 'save_entry'


@dataclass
class UpdateEntryBody(GenericEventBody):
    entry_id: str
    content: str
    job_id: str
    job_type: str = JobType.UPDATE_ENTRY
    event_type: str = 'update_entry'


@dataclass
class VSQueryBody(GenericEventBody):
    archive_id: str
    query_id: str
    query_str: str
    parent_job_id: str
    parent_job_type: str
    vector_store_ids: List[str]
    event_type: str = 'vs_query'


@dataclass
class VectorStoreTagRecalculation(GenericEventBody):
    archive_id: str
    vector_store_id: str
    event_type: str = 'recalculate_vector_tags'