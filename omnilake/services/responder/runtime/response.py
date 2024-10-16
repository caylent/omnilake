'''
Handle final responses
'''
import logging

from dataclasses import dataclass
from datetime import datetime, UTC as utc_tz
from typing import Dict

from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.ai import AI
from omnilake.internal_lib.clients import RawStorageManager
from omnilake.internal_lib.event_definitions import (
    GenericEventBody,
    IndexEntryBody,
)
from omnilake.internal_lib.naming import OmniLakeResourceName

from omnilake.tables.entries.client import Entry, EntriesClient
from omnilake.tables.jobs.client import JobsClient
from omnilake.tables.information_requests.client import (
    InformationRequestsClient,
    InformationRequestStatus,
)

# TODO: Temporary import, need to address a more maintainable solution in the future. BAD JIM!
from omnilake.services.ingestion.runtime.entry_creation import extract_insights


class ResponsePrompt:
    """
    Response prompt
    """

    PROMPT_INSTRUCTION = """Based on the information provided, provide a response based on the desired goal.
- The response should be clear and concise.
- The response should be accurate based on the information provided.
- The response should be relevant to the request.
- Follow any specific instructions provided in the's goal.

If you do not feel there is sufficient information to provide a response, simply specify "Insufficient information for response"
"""

    def __init__(self, goal: str, resource_name: str):
        self._resource_name = resource_name

        self.goal = goal

        self._entries_client = EntriesClient()

        self._storage_manager = RawStorageManager()

    def _get_resource_content(self, resource_name: str) -> str:
        '''
        Gets the content of the resource.

        Keyword arguments:
        resource_name -- The name of the resource
        '''
        parsed_resource_name = OmniLakeResourceName.from_string(resource_name)

        if parsed_resource_name.resource_type != "entry":
            raise ValueError(f"Resource type {parsed_resource_name.resource_type} is not currently supported.")

        content_resp = self._storage_manager.get_entry(entry_id=parsed_resource_name.resource_id)

        if content_resp.status_code >= 400:
            raise ValueError(f"Entry content with ID {parsed_resource_name.resource_id} could not be retrieved.")

        content = content_resp.response_body.get('content')

        if not content:
            raise ValueError(f"Entry with ID {parsed_resource_name.resource_id} is empty.")

        return content

    def generate(self):
        '''
        Generates the response prompt.
        '''
        content = self._get_resource_content(self._resource_name)

        full_prompt_lst = [
            self.PROMPT_INSTRUCTION,
            f"\n\nGOAL:\n\n{self.goal}",
            f"\n\nCONTENT:\n\n{content}"
        ]

        return "\n".join(full_prompt_lst)

    def to_str(self):
        '''
        Converts the response prompt to a string.
        '''
        return self.generate()


@dataclass
class FinalResponseEventBody(GenericEventBody):
    parent_job_id: str
    parent_job_type: str
    request_id: str
    source_resource_name: str


@fn_event_response(exception_reporter=ExceptionReporter(), logger=Logger("omnilake.services.responder.final_response"),
                   function_name="final_responder")
def final_responder(event: Dict, context: Dict) -> None:
    """
    Final responder function
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = FinalResponseEventBody(**source_event.body)

    jobs = JobsClient()

    parent_job = jobs.get(job_type=event_body.parent_job_type, job_id=event_body.parent_job_id)

    final_resp_job = parent_job.create_child(job_type="FINAL_RESPONSE")

    jobs.put(parent_job)

    job_failure_message = 'Failed to process final response'

    with jobs.job_execution(parent_job, failure_status_message=job_failure_message, skip_initialization=True):

        with jobs.job_execution(final_resp_job, failure_status_message=job_failure_message):

            logging.debug(f'Processing final response: {event_body}')

            info_request_objects = InformationRequestsClient()

            info_request_obj = info_request_objects.get(request_id=event_body.request_id)

            final_response_prompt = ResponsePrompt(
                goal=info_request_obj.goal,
                resource_name=event_body.source_resource_name
            )

            prompt = final_response_prompt.to_str()

            logging.debug(f'Final response prompt: {prompt}')

            ai = AI()

            final_response = ai.invoke(prompt=prompt, max_tokens=8000)

            logging.debug(f'Response result: {final_response}')

            final_resp_job.ai_statistics.invocations.append(final_response.statistics)

            logging.debug(f'AI Response: {final_response.response}')

            #extract insights
            insights, insight_invocation_resp = extract_insights(final_response.response)

            final_resp_job.ai_statistics.invocations.append(insight_invocation_resp.statistics)

            entries = EntriesClient()

            entry = Entry(
                analysis_scores={
                    'completeness': insights['completeness_score'],
                },
                content_hash=Entry.calculate_hash(final_response.response),
                effective_on=datetime.now(tz=utc_tz),
                sources=set([event_body.source_resource_name]),
                tags=[tag.lower().strip() for tag in insights['tags'].split(',')],
            )

            entries.put(entry)

            raw_storage = RawStorageManager()

            resp = raw_storage.save_entry(entry_id=entry.entry_id, content=final_response.response)

            logging.debug(f'Raw storage response: {resp}')

            info_request_obj.request_status = InformationRequestStatus.COMPLETED

            info_request_obj.entry_id = entry.entry_id

            info_request_obj.response_completed_on = datetime.now(tz=utc_tz)

            info_request_objects.put(info_request_obj)

    logging.debug(f'Final response job completed: {final_resp_job.job_id}')

    # Index the entry if a destination archive ID is provided
    if info_request_obj.destination_archive_id:
        event_mgr = EventPublisher()

        event_mgr.submit(
                event=EventBusEvent(
                    event_type=IndexEntryBody.event_type,
                    body=IndexEntryBody(
                        archive_id=info_request_obj.destination_archive_id,
                        entry_id=entry.entry_id,
                    ).to_dict()
                ),
            )