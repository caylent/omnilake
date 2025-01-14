"""
Handles the processing of new entries and adds them to the storage.
"""
import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict, Optional, Tuple

from da_vinci.core.immutable_object import ObjectBody
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

from omnilake.internal_lib.clients import AIStatisticSchema, AIStatisticsCollector

from omnilake.tables.provisioned_archives.client import ArchivesClient
from omnilake.tables.indexed_entries.client import IndexedEntriesClient
from omnilake.tables.jobs.client import JobsClient, JobStatus

from omnilake.constructs.archives.basic.runtime.event_definitions import (
    BasicArchiveGenerateEntryTagsEventBodySchema,
)


def extract_tags(content: str, tag_hint: Optional[str] = None, tag_model_id: Optional[str] = None,
                 tag_model_params: Optional[Dict] = None) -> Tuple[Dict, AIInvocationResponse]:
    """
    Uses AI to extract tags from the content.

    Keyword arguments:
    content -- The content to extract insights from
    tag_hint -- Special tagging instructions
    tag_model_id -- The model ID used for tagging
    tag_model_params -- The model parameters used for tagging
    """
    ai = AI()

    prompt_definition = """Extract relevant tags from the given content, focusing on:

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
- Review and refine the tag list, ensuring a balanced representation of the content"""

    if tag_hint:
        prompt_definition = f"{prompt_definition}\n\nSPECIAL TAGGING INSTRUCTIONS: {tag_hint}"

    response_definition = AIResponseDefinition(
        insights=[
            AIResponseInsightDefinition(
                name="tags",
                definition=prompt_definition,
            ),
        ]
    )

    prompt = response_definition.to_prompt(content)

    model_params = tag_model_params or {}

    result = ai.invoke(
        model_id=tag_model_id or ModelIDs.HAIKU,
        prompt=prompt,
        **model_params,
    )

    parser = ResponseParser()

    parser.feed(result.response)

    return parser.parsed_insights(), result


_FN_NAME = "omnilake.constructs.archives.basic.entry_tag_extration" 

@fn_event_response(function_name=_FN_NAME, exception_reporter=ExceptionReporter(),
                   logger=Logger(_FN_NAME))
def handler(event: Dict, context: Dict):
    """
    Processes the new entries and adds them to the storage.
    """
    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(body=source_event.body, schema=BasicArchiveGenerateEntryTagsEventBodySchema)

    jobs = JobsClient()

    parent_job_type = event_body.get('parent_job_type')

    parent_job_id = event_body.get('parent_job_id')

    parent_job = jobs.get(job_type=parent_job_type, job_id=parent_job_id)

    tag_extraction_job = parent_job.create_child(job_type='ENTRY_TAG_EXTRACTION')

    jobs.put(parent_job)

    entries = IndexedEntriesClient()

    with jobs.job_execution(tag_extraction_job, fail_parent=False):
        archive_id = event_body.get('archive_id')

        entry_id = event_body.get('entry_id')

        entry = entries.get(archive_id=archive_id, entry_id=entry_id)

        archives = ArchivesClient()

        archive = archives.get(archive_id=archive_id)

        content = event_body.get("content")

        insights, invocation_resp = extract_tags(
            content=content,
            tag_hint=archive.configuration.get("tag_hint_instructions"),
            tag_model_id=archive.configuration.get("tag_model_id"),
        )

        logging.debug(f"Invocation response: {invocation_resp}")

        stats_collector = AIStatisticsCollector()

        ai_statistic = ObjectBody(
            body={
                "job_type": parent_job_type,
                "job_id": parent_job_id,
                "model_id": invocation_resp.statistics.model_id,
                "total_output_tokens": invocation_resp.statistics.output_tokens,
                "total_input_tokens": invocation_resp.statistics.input_tokens,
            },
            schema=AIStatisticSchema,
        )

        stats_collector.publish(statistic=ai_statistic)

        entry.tags = [tag.lower().strip() for tag in insights['tags'].split(',')]

        entries.put(entry)

        logging.debug(f"Tags complete")

    parent_job.status = JobStatus.COMPLETED

    parent_job.ended = datetime.now(utc_tz)

    jobs.put(parent_job)

    logging.debug(f"Finished parent job")