'''
Summarizes the content into a more concise form.
'''
import logging

from dataclasses import dataclass
from datetime import datetime, UTC as utc_tz
from typing import Dict, List

from da_vinci.core.global_settings import setting_value
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.ai import AI, ModelIDs
from omnilake.internal_lib.clients import RawStorageManager
from omnilake.internal_lib.event_definitions import GenericEventBody
from omnilake.internal_lib.naming import OmniLakeResourceName, EntryResourceName

from omnilake.tables.summary_jobs.client import SummaryJobsTableClient
from omnilake.tables.entries.client import Entry, EntriesClient
from omnilake.tables.jobs.client import JobsClient
from omnilake.tables.information_requests.client import InformationRequestsClient

from omnilake.services.responder.runtime.response import FinalResponseEventBody


@dataclass
class SummarizationCompleted(GenericEventBody):
    '''
    Event body for summary completion.
    '''
    request_id: str # The Information Request ID that this context is associated with.
    resource_name: str # The name of the resource that was summarized.
    parent_job_id: str # The parent job ID.
    parent_job_type: str # The parent job type.


@dataclass
class SummarizationRequest(GenericEventBody):
    '''
    Event body for summary request.
    '''
    request_id: str # The Information Request ID that this context is associated with.
    resource_names: List[str] # The names of the resources to summary.
    parent_job_id: str # The parent job ID.
    parent_job_type: str # The parent job type.
    goal: str = None # The user goal.


class SummaryPrompt:
    def __init__(self, resource_names: List[str], goal: str = None):
        self.goal = goal

        self.resource_names = resource_names

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

    def resource_content(self, resource_name: str) -> str:
        '''
        Gets the content of the resource.

        Keyword arguments:
        resource_name -- The name of the resource
        '''
        content = self._get_resource_content(resource_name)

        full_content = f"{resource_name}\n\n{content}\n\n"

        return full_content

    def generate(self) -> str:
        '''
        Generates the summarize prompt.
        '''
        prompt = setting_value(namespace="responder", setting_key="default_summary_prompt")

        if self.goal:
            prompt += f"\n\nUSER GOAL: {self.goal}\n\n"

        prompt += "\n\nCONTENT:\n\n"

        resource_contents = "\n\n".join([self.resource_content(resource_name) for resource_name in self.resource_names])

        prompt += resource_contents

        return prompt

    def to_str(self):
        '''
        Converts the prompt to a string.
        '''
        return self.generate()


@fn_event_response(exception_reporter=ExceptionReporter(), function_name="response_summary_watcher",
                   logger=Logger("omnilake.services.responder.summary_watcher"))
def summary_watcher(event: Dict, context: Dict):
    '''
    Watches for summary events and triggers the summary process.
    '''
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = SummarizationCompleted(**source_event.body)

    summary_jobs = SummaryJobsTableClient()

    summary_jobs.add_completed_resource(
        request_id=event_body.request_id,
        resource_name=event_body.resource_name,
    )

    summarization_job = summary_jobs.get(request_id=event_body.request_id, consistent_read=True)

    event_bus = EventPublisher()

    if summarization_job.remaining_processes != 0:
        logging.debug(f'Summary job {summarization_job.request_id} still has {summarization_job.remaining_processes} remaining processes.')
        return

    if len(summarization_job.current_run_completed_resource_names) == 1:
        logging.info(f'summary job {summarization_job.request_id} has completed all processes.')

        event_bus.submit(
            event=EventBusEvent(
                body=FinalResponseEventBody(
                    request_id=event_body.request_id,
                    source_resource_name=list(summarization_job.current_run_completed_resource_names)[0],
                    parent_job_id=summarization_job.parent_job_id,
                    parent_job_type=summarization_job.parent_job_type,
                ).to_dict(),
                event_type="final_response",
            )
        )

        logging.info(f'Final response event submitted for summary job {summarization_job.request_id}.')

        return

    maximum_recursion_depth = setting_value(namespace="responder", setting_key="summary_maximum_recursion_depth")

    if summarization_job.current_run > maximum_recursion_depth:
        raise Exception(f'Summary job {summarization_job.request_id} has exceeded the maximum recursion depth.')

    summarization_job.current_run += 1

    latest_completed_resources_lst = list(summarization_job.current_run_completed_resource_names)

    max_content_group_size = setting_value(namespace="responder", setting_key="max_content_group_size")

    # Group the resources into the maximum content group size. Sorry for the lack of readability - Jim
    # Plus it's a fish ... Grouper ... I'll see myself out.
    grouper = lambda lst, n: [lst[i:i + n] for i in range(0, len(lst), n)]

    summary_groups = grouper(latest_completed_resources_lst, max_content_group_size)

    logging.debug(f'Summary groups: {summary_groups}')

    processes = 0

    information_requests = InformationRequestsClient()

    information_request = information_requests.get(request_id=event_body.request_id)

    summarization_job.current_run_completed_resource_names = set()

    for group in summary_groups:
        if len(group) == 1:
            logging.debug(f'Group of 1, adding directly to finished resources.')

            summarization_job.current_run_completed_resource_names.add(group[0])

            continue

        logging.debug(f'Group of {len(group)} resources, submitting for summarization.')

        processes += 1

        event_bus.submit(
            event=EventBusEvent(
                body=SummarizationRequest(
                    request_id=event_body.request_id,
                    resource_names=list(group),
                    goal=information_request.goal,
                    parent_job_id=summarization_job.parent_job_id,
                    parent_job_type=summarization_job.parent_job_type,
                ).to_dict(),
                event_type="begin_summarization",
            )
        )

    summarization_job.remaining_processes = processes

    summary_jobs.put(summarization_job)


@fn_event_response(exception_reporter=ExceptionReporter(), function_name="summarize_resources",
                   logger=Logger("omnilake.services.responder.resource_summarizer"))
def summarize_resources(event: Dict, context: Dict):
    '''
    Summarizes the content of the resources.
    '''
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = SummarizationRequest(**source_event.body)

    summary_prompt = SummaryPrompt(
        goal=event_body.goal,
        resource_names=event_body.resource_names
    )

    jobs = JobsClient()

    parent_job = jobs.get(job_type=event_body.parent_job_type, job_id=event_body.parent_job_id)

    child_job = parent_job.create_child(job_type="DATA_SUMMARIZATION")

    jobs.put(child_job)

    jobs.put(parent_job)

    with jobs.job_execution(parent_job, failure_status_message="Summary job failed", skip_completion=True):

        with jobs.job_execution(child_job, failure_status_message="Summary job failed"):
            logging.debug(f'Summarizing resources: {event_body.resource_names}')

            prompt = summary_prompt.to_str()

            logging.debug(f'Summary prompt: {prompt}')

            ai = AI(default_model_id=ModelIDs.HAIKU)

            summarization_result = ai.invoke(prompt=prompt, max_tokens=8000)

            logging.debug(f'Summarization result: {summarization_result}')

            child_job.ai_statistics.invocations.append(summarization_result.statistics)

            logging.debug(f'AI Response: {summarization_result.response}')

            entries = EntriesClient()

            entry = Entry(
                char_count=len(summarization_result.response),
                content_hash=Entry.calculate_hash(summarization_result.response),
                effective_on=datetime.now(tz=utc_tz),
                sources=set(event_body.resource_names),
            )

            entries.put(entry)

            raw_storage = RawStorageManager()

            resp = raw_storage.save_entry(entry_id=entry.entry_id, content=summarization_result.response)

            logging.debug(f'Raw storage response: {resp}')

    event_bus = EventPublisher()

    event_bus.submit(
        event=EventBusEvent(
            body=SummarizationCompleted(
                request_id=event_body.request_id,
                resource_name=str(EntryResourceName(resource_id=entry.entry_id)),
                parent_job_id=event_body.parent_job_id,
                parent_job_type=event_body.parent_job_type,
            ).to_dict(),
            event_type="summary_completed",
        )
    )