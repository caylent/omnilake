'''
Handles the processing of new entries and adds them to the storage.
'''
import logging

from dataclasses import dataclass
from typing import Dict, Tuple, Union

from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.ai import AI, ModelIDs, AIInvocationResponse
from omnilake.internal_lib.ai_insights import (
    AIResponseDefinition,
    AIResponseInsightDefinition,
    ResponseParser,
)
from omnilake.internal_lib.event_definitions import (
    GenericEventBody,
)
from omnilake.internal_lib.job_types import JobType

from omnilake.tables.entries.client import EntriesClient
from omnilake.tables.jobs.client import JobsClient


@dataclass
class GenerateEntryTagsBody(GenericEventBody):
    """
    The body of the generate_entry_tags event.
    """
    entry_id: str
    content: str
    parent_job_id: str
    parent_job_type: Union[str, JobType]
    event_type: str = 'generate_entry_tags'


def extract_tags(content: str) -> Tuple[Dict, AIInvocationResponse]:
    """
    Uses AI to extract tags from the content.

    Keyword arguments:
    content -- The content to extract insights from
    """
    ai = AI()

    response_definition = AIResponseDefinition(
        insights=[
            AIResponseInsightDefinition(
                name="tags",
                definition="""Extract relevant tags from the given content, focusing on:

- Proper names (people, places, organizations, products)
- Specific categories or themes
- Key concepts or topics
- Business categories or industries
- Subjects or disciplines (e.g., science, history, art)
- Time periods or eras
- Emotions or sentiments expressed
- Technical terms or jargon
- Cultural references
- Target audience or demographic

Guidelines:

- Provide tags as a comma-separated list
- Include both specific and broader tags where appropriate
- Aim for concise, descriptive tags (1-3 words each)
- Prioritize tags that would be most useful for categorization or search purposes
- Consider the context and main focus of the content when selecting tags
- If applicable, include tags in different languages that are relevant to the content
- Aim to capture the main themes rather than every minor detail

Tagging approach:

- First, read through the entire content to understand the overall context
- Identify the primary topic or theme
- Extract tags based on the categories listed above
- Review and refine the tag list, ensuring a balanced representation of the content""",
            ),
        ]
    )

    prompt = response_definition.to_prompt(content)

    result = ai.invoke(
        model_id=ModelIDs.HAIKU,
        prompt=prompt,
    )

    parser = ResponseParser()

    parser.feed(result.response)

    return parser.parsed_insights(), result


@fn_event_response(function_name='entry_tag_extraction', exception_reporter=ExceptionReporter(), logger=Logger("omnilake.storage.vector.entry_tag_extraction"))
def handler(event: Dict, context: Dict):
    """
    Processes the new entries and adds them to the storage.
    """
    source_event = EventBusEvent.from_lambda_event(event)

    event_body = GenerateEntryTagsBody(**source_event.body)

    jobs = JobsClient()

    job = jobs.get(job_type=event_body.parent_job_type, job_id=event_body.parent_job_id)

    tag_extraction_job = job.create_child(job_type='ENTRY_TAG_EXTRACTION')

    entries = EntriesClient()

    with jobs.job_execution(tag_extraction_job, failure_status_message='Failed to extract tags', fail_all_parents=True):

        entry = entries.get(entry_id=event_body.entry_id)

        insights, invocation_resp = extract_tags(event_body.content)

        logging.debug(f"Invocation response: {invocation_resp}")

        tag_extraction_job.ai_statistics.invocations.append(invocation_resp.statistics)

        logging.debug(f"Extracted insights: {insights}")

        entry.tags = [tag.lower().strip() for tag in insights['tags'].split(',')]

        entries.put(entry)