"""
Handles the final response, using the extracted information against the requested goal
"""
import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict
from uuid import uuid4

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

from omnilake.internal_lib.event_definitions import (
    LakeRequestInternalResponseEventBodySchema,
)

from omnilake.internal_lib.naming import EntryResourceName

from omnilake.tables.jobs.client import JobsClient

from omnilake.constructs.processors.knowledge_graph.runtime.event_definitions import (
    FinalResponseRequestSchema,
)

from omnilake.constructs.processors.knowledge_graph.tables.knowledge_graph_jobs.client import (
    KnowledgeGraphJobClient,
)


class ResponsePrompt:
    prompt_instructions = """Given the provided information, please respond in the most optimal way to achieve the user's goal.

Goal:
{goal}

Information:
{information}
"""

    def __init__(self, entry_id: str, goal: str):
        """
        Extraction prompt constructor.

        Keyword arguments:
        entry_id -- The ID of the entry to extract knowledge from.
        """
        self.entry_id = entry_id

        self.goal = goal

        self.storage_manager = RawStorageManager()

    def to_str(self) -> str:
        '''
        Prompts the user to extract knowledge from the resource.
        '''
        content_resp = self.storage_manager.get_entry(entry_id=self.entry_id)

        if content_resp.status_code >= 400:
            raise ValueError(f"Entry content with ID {self.entry_id} could not be retrieved.")

        content = content_resp.response_body.get('content')

        return self.prompt_instructions.format(goal=self.goal, information=content)


_FN_NAME = "omnilake.constructs.processors.knowledge_graph.response"


@fn_event_response(exception_reporter=ExceptionReporter(), function_name=_FN_NAME, logger=Logger(_FN_NAME))
def handler(event: Dict, context: Dict):
    '''
    Handles the final response, using the extracted information against the requested goal
    '''
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=FinalResponseRequestSchema,
    )

    entry_id = event_body["entry_id"]

    jobs = JobsClient()

    parent_job = jobs.get(job_type=event_body.get("parent_job_type"), job_id=event_body.get("parent_job_id"))

    with jobs.job_execution(parent_job, skip_initialization=True, failure_status_message="Final response failed"):
        logging.debug('Executing final response')

        response_prompt = ResponsePrompt(entry_id=entry_id, goal=event_body["goal"])

        prompt = response_prompt.to_str()

        logging.debug(f'Response prompt: {prompt}')

        ai = AI()

        response_result = ai.invoke(prompt=prompt, max_tokens=8000, model_id=event_body.get("model_id"))

        logging.debug(f'AI Response: {response_result.response}')

        raw_storage = RawStorageManager()

        resp = raw_storage.create_entry(
            content=response_result.response,
            effective_on=datetime.now(tz=utc_tz),
            sources=[str(EntryResourceName(entry_id))]
        )

        entry_id = resp.response_body["entry_id"]

        logging.debug(f'Raw storage response: {resp}')

        stats_collector = AIStatisticsCollector()

        invocation_id = str(uuid4())

        ai_statistic = ObjectBody(
            body={
                "job_type": parent_job.job_type,
                "job_id": parent_job.job_id,
                "invocation_id": invocation_id,
                "model_id": response_result.statistics.model_id,
                "resulting_entry_id": entry_id,
                "total_output_tokens": response_result.statistics.output_tokens,
                "total_input_tokens": response_result.statistics.input_tokens,
            },
            schema=AIStatisticSchema,
        )

        stats_collector.publish(statistic=ai_statistic)

        event_bus = EventPublisher()

        kg_jobs = KnowledgeGraphJobClient()

        kg_request_id = event_body["knowledge_graph_processing_id"]

        kg_job = kg_jobs.get(knowledge_graph_request_id=kg_request_id)

        invocation_ids = list(kg_job.ai_invocation_ids)

        invocation_ids.append(invocation_id)

        final_body = ObjectBody(
            body={
                "ai_invocation_ids": invocation_ids,
                "entry_ids": [entry_id],
                "lake_request_id": kg_job.lake_request_id,
            },
            schema=LakeRequestInternalResponseEventBodySchema,
        )

        event_bus.submit(
            event=source_event.next_event(
                body=final_body.to_dict(),
                event_type=final_body["event_type"],
            )
        )