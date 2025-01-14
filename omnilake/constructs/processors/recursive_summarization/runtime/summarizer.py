"""
Summarizes the content into a more concise form.
"""
import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict, List, Optional
from uuid import uuid4

from da_vinci.core.global_settings import setting_value
from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.ai import AI
from omnilake.internal_lib.clients import (
    AIStatisticSchema,
    AIStatisticsCollector,
    RawStorageManager,
)
from omnilake.internal_lib.naming import OmniLakeResourceName, EntryResourceName

from omnilake.tables.entries.client import Entry, EntriesClient
from omnilake.tables.jobs.client import JobsClient
from omnilake.tables.sources.client import SourcesClient

from omnilake.constructs.processors.recursive_summarization.runtime.event_definitions import (
    SummarizationCompletedSchema,
    SummarizationRequestSchema,
)


class SummaryPrompt:
    def __init__(self, entry_ids: List[str], custom_prompt: str = None, goal: str = None,
                 include_source_metadata: Optional[bool] = False):
        """
        Summary prompt constructor.

        Keyword arguments:
        custom_prompt -- The custom prompt to use.
        entry_ids -- The IDs of the entries to summarize.
        goal -- The user goal.
        include_source_metadata -- Whether to include the original source metadata.
        """
        self.custom_prompt = custom_prompt

        self.goal = goal

        self.entry_ids = entry_ids 

        self._inclue_original_source_metadata = include_source_metadata

        if self._inclue_original_source_metadata:
            self._sources_client = SourcesClient()

        self._entries_client = EntriesClient()

        self._storage_manager = RawStorageManager()

    def _get_resource_content(self, entry_id: str) -> str:
        '''
        Gets the content of the resource.

        Keyword arguments:
        resource_name -- The name of the resource
        '''
        content = ''

        # If source metadata is requested, include it in the content retrieval from the sources table
        if self._inclue_original_source_metadata:
            entry_obj = self._entries_client.get(entry_id=entry_id)

            if entry_obj.original_of_source:
                logging.debug(f"Source metadata requested for entry {entry_id}.")

                source_rn = OmniLakeResourceName.from_string(entry_obj.original_of_source)

                source_obj = self._sources_client.get(
                    source_type=source_rn.resource_id.source_type,
                    source_id=source_rn.resource_id.source_id,
                )

                content += f"SOURCE METADATA:\n\n{source_obj.source_arguments}\n\nCONTENT:\n\n"

        content_resp = self._storage_manager.get_entry(entry_id=entry_id)

        if content_resp.status_code >= 400:
            raise ValueError(f"Entry content with ID {entry_id} could not be retrieved.")

        content += content_resp.response_body.get('content')

        if not content:
            raise ValueError(f"Entry with ID {entry_id} is empty.")

        return content

    def resource_content(self, entry_id: str) -> str:
        '''
        Gets the content of the resource.

        Keyword arguments:
        entry_id -- The ID of the entry
        '''
        content = self._get_resource_content(entry_id=entry_id)

        full_content = f"{entry_id}\n\n{content}\n\n"

        return full_content

    def generate(self, custom_prompt: Optional[str] = None) -> str:
        '''
        Generates the summarize prompt.
        '''
        if custom_prompt:
            prompt = custom_prompt

        else:
            prompt = setting_value(
                namespace="omnilake::recursive_summarization_construct",
                setting_key="default_summary_prompt",
            )

            prompt += f"\n\nUSER GOAL: {self.goal}\n\n"

        resource_contents = "\n\n".join([self.resource_content(entry_id) for entry_id in self.entry_ids])

        prompt += resource_contents

        return prompt

    def to_str(self):
        '''
        Converts the prompt to a string.
        '''
        return self.generate(self.custom_prompt)


_FN_NAME = "omnilake.constructs.processors.recursive_summarization.summarizer"


@fn_event_response(exception_reporter=ExceptionReporter(), function_name=_FN_NAME,
                   logger=Logger(_FN_NAME))
def handler(event: Dict, context: Dict):
    '''
    Summarizes the content of the resources.
    '''
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=SummarizationRequestSchema,
    )

    summary_prompt = SummaryPrompt(
        entry_ids=event_body.get("entry_ids"),
        goal=event_body.get("goal"),
        include_source_metadata=event_body.get("include_source_metadata"),
        custom_prompt=event_body.get("prompt"),
    )

    jobs = JobsClient()

    parent_job = jobs.get(job_type=event_body.get("parent_job_type"), job_id=event_body.get("parent_job_id"))

    logging.debug('Setting up job')

    child_job = parent_job.create_child(job_type="RECURSIVE_SUMMARIZATION_PROCESSING")

    entry_ids = event_body.get("entry_ids")

    with jobs.job_execution(child_job, failure_status_message="Summary job failed"):
        logging.debug(f'Summarizing resources: {entry_ids}')

        prompt = summary_prompt.to_str()

        logging.debug(f'Summary prompt: {prompt}')

        ai = AI()

        summarization_result = ai.invoke(prompt=prompt, max_tokens=8000, model_id=event_body.get("model_id"))

        logging.debug(f'AI Response: {summarization_result.response}')

        entries = EntriesClient()

        sources = [str(EntryResourceName(e_id)) for e_id in event_body.get("entry_ids")]

        entry = Entry(
            char_count=len(summarization_result.response),
            content_hash=Entry.calculate_hash(summarization_result.response),
            effective_on=datetime.now(tz=utc_tz),
            sources=set(sources),
        )

        entries.put(entry)

        stats_collector = AIStatisticsCollector()

        invocation_id = str(uuid4())

        ai_statistic = ObjectBody(
            body={
                "job_type": parent_job.job_type,
                "job_id": parent_job.job_id,
                "invocation_id": invocation_id,
                "model_id": summarization_result.statistics.model_id,
                "resulting_entry_id": entry.entry_id,
                "total_output_tokens": summarization_result.statistics.output_tokens,
                "total_input_tokens": summarization_result.statistics.input_tokens,
            },
            schema=AIStatisticSchema,
        )

        stats_collector.publish(statistic=ai_statistic)

        raw_storage = RawStorageManager()

        resp = raw_storage.save_entry(entry_id=entry.entry_id, content=summarization_result.response)

        logging.debug(f'Raw storage response: {resp}')

        event_bus = EventPublisher()

        completed_body = ObjectBody(
            body={
                "ai_invocation_id": invocation_id,
                "entry_id": entry.entry_id,
                "summary_request_id": event_body["summary_request_id"],
            },
            schema=SummarizationCompletedSchema,
        )

        logging.debug(f'Publishing completed body: {completed_body.to_dict()}')

        event_bus.submit(
            event=source_event.next_event(
                body=completed_body.to_dict(),
                event_type=completed_body["event_type"],
            )
        )