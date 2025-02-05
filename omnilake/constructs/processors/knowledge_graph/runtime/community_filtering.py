import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict, Optional
from uuid import uuid4

from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.ai import AI, ModelIDs
from omnilake.internal_lib.clients import (
    AIStatisticSchema,
    AIStatisticsCollector,
    RawStorageManager,
)
from omnilake.internal_lib.naming import EntryResourceName

from omnilake.tables.jobs.client import JobsClient

from omnilake.constructs.processors.knowledge_graph.runtime.event_definitions import (
    KnowledgeAIFilteringCompleteSchema,
    KnowledgeAIFilteringRequestSchema,
)


class FilterPrompt:
    prompt_instructions = """Your task is to filter and clean a knowledge graph, converting the relationships to triples. Focus on three key tasks:

1. Remove irrelevant/non-useful information: 
- Remove nodes/relationships that don't contribute meaningful information
- Keep information that adds context or valuable detail
- Each retained relationship should serve a clear purpose

2. Resolve contradictions:
- Identify contradicting statements
- Keep the relationship with higher weighted source
- Weight indicates confidence/frequency of the relationship

3. Deduplicate similar relationships:
- Combine relationships that express the same meaning in different words
- Standardize relationship phrasing
- Keep the most clear/concise version

Input format:
node1 -relationship-> node2 (weight: N)

Output format:
- Return as triples: node1|relationship|node2
- One triple per line
- Return empty string if no useful information remains

Example input:
user -requests-> data (weight: 3)
user -asks for-> data (weight: 1)
user -does not want-> data (weight: 1)
processor -handles-> data (weight: 1)
processor -dislikes-> coffee (weight: 1)

Example output:
user|requests|data
processor|handles|data
processor|dislikes|coffee

{goal_instructions}

Please filter and convert the following knowledge graph to triples:
"""

    def __init__(self, entry_id: str, goal: Optional[str] = None):
        """
        Filter prompt constructor.

        Keyword arguments:
        entry_id -- The ID of the entry to filter with AI.
        goal -- The goal of the filtering.
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

        if self.goal:
            goal_instructions = f"Filter the information based on how it might help with the users's stated goal: \"{self.goal}\""

            prompt_instructions = self.prompt_instructions.format(goal_instructions=goal_instructions)

        else:
            prompt_instructions = self.prompt_instructions.format(goal_instructions="")

        return f"{prompt_instructions}\n\n{content}"


_FN_NAME = "omnilake.constructs.processors.knowledge_graph.community_filter_execution"


@fn_event_response(exception_reporter=ExceptionReporter(), function_name=_FN_NAME, logger=Logger(_FN_NAME))
def handler(event: Dict, context: Dict):
    '''
    Filters the provided knowledge graph communities.
    '''
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=KnowledgeAIFilteringRequestSchema,
    )

    entry_id = event_body["entry_id"]

    jobs = JobsClient()

    parent_job = jobs.get(job_type=event_body.get("parent_job_type"), job_id=event_body.get("parent_job_id"))

    logging.debug('Setting up job')

    child_job = parent_job.create_child(job_type="KNOWLEDGE_GRAPH_FILTERING")

    with jobs.job_execution(child_job, failure_status_message="Extraction job failed"):
        logging.debug(f'Extracting knowledge from resource: {entry_id}')

        goal = event_body.get("goal")

        extraction_prompt = FilterPrompt(entry_id=entry_id, goal=goal)

        prompt = extraction_prompt.to_str()

        logging.debug(f'Filter prompt: {prompt}')

        ai = AI(default_model_id=ModelIDs.HAIKU)

        filter_result = ai.invoke(prompt=prompt, max_tokens=8000, model_id=event_body.get("model_id"))

        logging.debug(f'AI Response: {filter_result.response}')

        raw_storage = RawStorageManager()

        resp = raw_storage.create_entry(
            content=filter_result.response,
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
                "model_id": filter_result.statistics.model_id,
                "resulting_entry_id": entry_id,
                "total_output_tokens": filter_result.statistics.output_tokens,
                "total_input_tokens": filter_result.statistics.input_tokens,
            },
            schema=AIStatisticSchema,
        )

        stats_collector.publish(statistic=ai_statistic)

        event_bus = EventPublisher()

        completed_body = ObjectBody(
            body={
                "ai_invocation_id": invocation_id,
                "entry_id": entry_id,
                "knowledge_graph_processing_id": event_body["knowledge_graph_processing_id"],
            },
            schema=KnowledgeAIFilteringCompleteSchema,
        )

        logging.debug(f'Publishing completed body: {completed_body.to_dict()}')

        event_bus.submit(
            event=source_event.next_event(
                body=completed_body.to_dict(),
                event_type=completed_body["event_type"],
            )
        )