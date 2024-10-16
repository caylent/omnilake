'''
Compacts the content into a more concise form.
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

from omnilake.tables.compaction_jobs.client import CompactionJobsTableClient
from omnilake.tables.entries.client import Entry, EntriesClient
from omnilake.tables.jobs.client import JobsClient, JobStatus
from omnilake.tables.information_requests.client import InformationRequestsClient

from omnilake.services.responder.runtime.response import FinalResponseEventBody


@dataclass
class CompactionCompleted(GenericEventBody):
    '''
    Event body for compaction completion.
    '''
    request_id: str # The Information Request ID that this context is associated with.
    resource_name: str # The name of the resource that was compacted.
    parent_job_id: str # The parent job ID.
    parent_job_type: str # The parent job type.


@dataclass
class CompactionRequest(GenericEventBody):
    '''
    Event body for compaction request.
    '''
    request_id: str # The Information Request ID that this context is associated with.
    resource_names: List[str] # The names of the resources to compact.
    parent_job_id: str # The parent job ID.
    parent_job_type: str # The parent job type.
    goal: str = None # The user goal.


class CompactionPrompt:
    NO_GOAL_PROMPT = """You are an AI assistant designed to compact data by extracting key facts and insights from given content. Your primary goal is to distill information to its most essential elements. Follow these steps:

- Scan the entire content to grasp its overall scope.
- Identify and extract:
    - Core facts and statistics
    - Key insights or conclusions
    - Essential data points
    - Critical findings or results
- Discard all supplementary information, explanations, and context unless absolutely crucial for understanding the key facts.
- Ensure each extracted element can stand alone as a piece of information.
- Maintain a high level of detail and accuracy in your summary. It should still be concise and to the point.
- Bring forward all details that are important

Your output should be a highly condensed version of the original content, retaining only the most crucial facts and insights. Aim for maximum information density while maintaining clarity and accuracy. 
"""

    WITH_GOAL_PROMPT = """You are an AI assistant designed to compact data by extracting key facts and insights from given content, with a specific focus on the user's stated goal. Your primary objective is to distill information to its most essential and relevant elements. Follow these steps:

- Carefully review the user's stated goal.
- Scan the entire content to identify information relevant to the user's needs.
- Extract and compact only the following elements that directly relate to the user's request and goal:
    - Core facts and statistics
    - Key insights or conclusions
    - Essential data points
    - Critical findings or results
- Discard all information that doesn't directly contribute to addressing the user's request or achieving their goal.
- Ensure each extracted element is directly relevant to the user's request and goal.
- Maintain a high level of detail and accuracy in your summary. It should still be concise and to the point.
- Bring forward all details that are important

Your output should be a highly condensed, goal-oriented version of the original content, retaining only the most crucial facts and insights that directly address the user's needs. Aim for maximum relevance and information density while maintaining clarity and accuracy.
"""
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
        Generates the compaction prompt.
        '''
        prompt = self.NO_GOAL_PROMPT

        if self.goal:
            prompt = self.WITH_GOAL_PROMPT

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


@fn_event_response(exception_reporter=ExceptionReporter(), function_name="compaction_watcher",
                   logger=Logger("omnilake.services.responder.compaction_watcher"))
def compaction_watcher(event: Dict, context: Dict):
    '''
    Watches for compaction events and triggers the compaction process.
    '''
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = CompactionCompleted(**source_event.body)

    compaction_jobs = CompactionJobsTableClient()

    compaction_jobs.add_completed_resource(
        request_id=event_body.request_id,
        resource_name=event_body.resource_name,
    )

    compaction_job = compaction_jobs.get(request_id=event_body.request_id, consistent_read=True)

    event_bus = EventPublisher()

    if compaction_job.remaining_processes != 0:
        logging.debug(f'Compaction job {compaction_job.request_id} still has {compaction_job.remaining_processes} remaining processes.')
        return

    if len(compaction_job.current_run_completed_resource_names) == 1:
        logging.info(f'Compaction job {compaction_job.request_id} has completed all processes.')

        event_bus.submit(
            event=EventBusEvent(
                body=FinalResponseEventBody(
                    request_id=event_body.request_id,
                    source_resource_name=list(compaction_job.current_run_completed_resource_names)[0],
                    parent_job_id=compaction_job.parent_job_id,
                    parent_job_type=compaction_job.parent_job_type,
                ).to_dict(),
                event_type="final_response",
            )
        )

        logging.info(f'Final response event submitted for compaction job {compaction_job.request_id}.')

        return

    maximum_recursion_depth = setting_value(namespace="responder", setting_key="compaction_maximum_recursion_depth")

    if compaction_job.current_run > maximum_recursion_depth:
        raise Exception(f'Compaction job {compaction_job.request_id} has exceeded the maximum recursion depth.')

    compaction_job.current_run += 1

    latest_completed_resources_lst = list(compaction_job.current_run_completed_resource_names)

    max_content_group_size = setting_value(namespace="responder", setting_key="max_content_group_size")

    # Group the resources into the maximum content group size. Sorry for the lack of readability - Jim
    # Plus it's a fish ... Grouper ... I'll see myself out.
    grouper = lambda lst, n: [lst[i:i + n] for i in range(0, len(lst), n)]

    compaction_groups = grouper(latest_completed_resources_lst, max_content_group_size)

    logging.debug(f'Compaction groups: {compaction_groups}')

    processes = 0

    information_requests = InformationRequestsClient()

    information_request = information_requests.get(request_id=event_body.request_id)

    compaction_job.current_run_completed_resource_names = set()

    for group in compaction_groups:
        if len(group) == 1:
            logging.debug(f'Group of 1, adding directly to finished resources.')

            compaction_job.current_run_completed_resource_names.add(group[0])

            continue

        logging.debug(f'Group of {len(group)} resources, submitting for compaction.')

        processes += 1

        event_bus.submit(
            event=EventBusEvent(
                body=CompactionRequest(
                    request_id=event_body.request_id,
                    resource_names=list(group),
                    goal=information_request.goal,
                    parent_job_id=compaction_job.parent_job_id,
                    parent_job_type=compaction_job.parent_job_type,
                ).to_dict(),
                event_type="begin_compaction",
            )
        )

    compaction_job.remaining_processes = processes

    compaction_jobs.put(compaction_job)


@fn_event_response(exception_reporter=ExceptionReporter(), function_name="compact_resources",
                   logger=Logger("omnilake.services.responder.resource_compaction"))
def compact_resources(event: Dict, context: Dict):
    '''
    Compacts the content of the resources.
    '''
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = CompactionRequest(**source_event.body)

    compaction_prompt = CompactionPrompt(
        goal=event_body.goal,
        resource_names=event_body.resource_names
    )

    jobs = JobsClient()

    parent_job = jobs.get(job_type=event_body.parent_job_type, job_id=event_body.parent_job_id)

    child_job = parent_job.create_child(job_type="DATA_COMPACTION")

    jobs.put(child_job)

    jobs.put(parent_job)

    with jobs.job_execution(parent_job, failure_status_message="Compaction job failed", skip_completion=True):

        with jobs.job_execution(child_job, failure_status_message="Compaction job failed"):
            logging.debug(f'Compacting resources: {event_body.resource_names}')

            prompt = compaction_prompt.to_str()

            logging.debug(f'Compaction prompt: {prompt}')

            ai = AI(default_model_id=ModelIDs.HAIKU)

            compaction_result = ai.invoke(prompt=prompt, max_tokens=8000)

            logging.debug(f'Compaction result: {compaction_result}')

            child_job.ai_statistics.invocations.append(compaction_result.statistics)

            logging.debug(f'AI Response: {compaction_result.response}')

            entries = EntriesClient()

            entry = Entry(
                char_count=len(compaction_result.response),
                content_hash=Entry.calculate_hash(compaction_result.response),
                effective_on=datetime.now(tz=utc_tz),
                sources=set(event_body.resource_names),
            )

            entries.put(entry)

            raw_storage = RawStorageManager()

            resp = raw_storage.save_entry(entry_id=entry.entry_id, content=compaction_result.response)

            logging.debug(f'Raw storage response: {resp}')

    event_bus = EventPublisher()

    event_bus.submit(
        event=EventBusEvent(
            body=CompactionCompleted(
                request_id=event_body.request_id,
                resource_name=str(EntryResourceName(resource_id=entry.entry_id)),
                parent_job_id=event_body.parent_job_id,
                parent_job_type=event_body.parent_job_type,
            ).to_dict(),
            event_type="compaction_completed",
        )
    )