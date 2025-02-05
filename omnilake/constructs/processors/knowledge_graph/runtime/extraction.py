"""
Summarizes the content into a more concise form.
"""
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
    KnowledgeExtractionCompleteSchema,
    KnowledgeExtractionRequestSchema,
)


class ExtractionPrompt:
    prompt_instructions = """Extract knowledge from the provided text and present it in a triple format using the structure:
Entity1|Relationship|Entity2

Rules:
- Each line should contain exactly three elements separated by vertical bars (|)
- The first and third elements should be nouns or noun phrases (Nodes)
- The middle element should be a verb or verb phrase describing the relationship (Edge)
- Present one triple per line
- Use clear, concise language
- Capture specific attributes when mentioned:
 - Full names (first, middle, last)
 - Titles or roles  
 - Organizations
 - Locations
 - Dates
 - Identifiers
 - Attributes
- Link related entities (e.g., Person|has name|John Smith\\nJohn Smith|works at|Company)
- Include quantifiable information when available (ages, years, amounts)

Requirements:
- Output only triples, no other text
- One triple per line
- First element = subject node
- Middle element = relationship/action
- Last element = object node  
- Use only | as separator

Example format:
Company|sells|Product
Employee|reports to|Manager
Software|integrates with|Database
John Smith|has role|CEO
John Smith|works at|Acme Corp
Acme Corp|located in|New York
Project X|started on|2024-01-15
function footion|has parameter|x
parameter x|is type|int

{goal_instructions}

Please extract and format all relevant knowledge triples from the following text:
"""

    def __init__(self, entry_id: str, goal: Optional[str] = None):
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

        if self.goal:
            goal_instructions = f"Extract information based on how it might help with the users's stated goal: \"{self.goal}\""

            prompt_instructions = self.prompt_instructions.format(goal_instructions=goal_instructions)

        else:
            prompt_instructions = self.prompt_instructions.format(goal_instructions="")

        return f"{prompt_instructions}\n\n{content}"


_FN_NAME = "omnilake.constructs.processors.knowledge_graph.extraction"


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
        schema=KnowledgeExtractionRequestSchema,
    )

    entry_id = event_body["entry_id"]

    jobs = JobsClient()

    parent_job = jobs.get(job_type=event_body.get("parent_job_type"), job_id=event_body.get("parent_job_id"))

    logging.debug('Setting up job')

    child_job = parent_job.create_child(job_type="KNOWLEDGE_GRAPH_EXTRACTION")

    with jobs.job_execution(child_job, failure_status_message="Extraction job failed"):
        logging.debug(f'Extracting knowledge from resource: {entry_id}')

        extraction_prompt = ExtractionPrompt(entry_id=entry_id, goal=event_body.get("goal"))

        prompt = extraction_prompt.to_str()

        logging.debug(f'Summary prompt: {prompt}')

        ai = AI(default_model_id=ModelIDs.HAIKU)

        summarization_result = ai.invoke(prompt=prompt, max_tokens=8000, model_id=event_body.get("model_id"))

        logging.debug(f'AI Response: {summarization_result.response}')

        raw_storage = RawStorageManager()

        resp = raw_storage.create_entry(
            content=summarization_result.response,
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
                "model_id": summarization_result.statistics.model_id,
                "resulting_entry_id": entry_id,
                "total_output_tokens": summarization_result.statistics.output_tokens,
                "total_input_tokens": summarization_result.statistics.input_tokens,
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
            schema=KnowledgeExtractionCompleteSchema,
        )

        logging.debug(f'Publishing completed body: {completed_body.to_dict()}')

        event_bus.submit(
            event=source_event.next_event(
                body=completed_body.to_dict(),
                event_type=completed_body["event_type"],
            )
        )