'''
Handles the processing of new entries and adds them to the storage.
'''
import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict, List, Tuple

from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.ai import AI, ModelIDs, AIInvocationResponse
from omnilake.internal_lib.ai_insights import (
    AIResponseDefinition,
    AIResponseInsightDefinition,
    ResponseParser,
)
from omnilake.internal_lib.clients import RawStorageManager
from omnilake.internal_lib.event_definitions import (
    AddEntryBody,
    IndexEntryBody,
)
from omnilake.internal_lib.job_types import JobType
from omnilake.internal_lib.naming import (
    OmniLakeResourceName,
    EntryResourceName,
)

from omnilake.tables.entries.client import Entry, EntriesClient
from omnilake.tables.jobs.client import JobsClient, JobStatus
from omnilake.tables.sources.client import SourcesClient


def _summarize_content(content: str) -> AIInvocationResponse:
    """
    Creates a summary of the given content

    Keyword arguments:
    content -- The content to summarize
    """
    ai = AI()

    result = ai.invoke(
        model_id=ModelIDs.SONNET,
        prompt=f"""Summarize the given content concisely, ensuring that:

- All important details are preserved
- Direct quotations are included and properly attributed
- Cited sources are mentioned
- Key arguments, findings, and conclusions are captured
- The summary is significantly shorter than the original text
- No new information is added
- Only provide a summary of the content without any additional commentary or analysis

Aim for clarity and brevity while maintaining the essence and accuracy of the original content.

CONTENT:
{content}""",
    )

    return result


def extract_insights(content: str) -> Tuple[Dict, AIInvocationResponse]:
    """
    Uses AI to extract insights from the content.

    Keyword arguments:
    content -- The content to extract insights from
    """
    ai = AI()

    response_definition = AIResponseDefinition(
        insights=[
            AIResponseInsightDefinition(
                name="completeness_score",
                definition="""Please evaluate the provided document for its completeness of information. Focus on the following aspects:

- Definitions: Are key terms, especially non-standard or domain-specific ones, clearly defined?
- Abbreviations: Are all abbreviations and acronyms explained?
- Context: Is sufficient background information provided for understanding the content?
- Depth: Are concepts explored thoroughly, or are they only superficially mentioned?
- Clarity: Is the information presented in a clear, understandable manner?

Based on these criteria, assign a score of LOW, MEDIUM, or HIGH:

- LOW: The document lacks many definitions, leaves abbreviations unexplained, provides little context, explores concepts superficially, or is unclear in its presentation.
- MEDIUM: The document defines some key terms, explains most abbreviations, provides some context, explores concepts with moderate depth, and is generally clear.
- HIGH: The document thoroughly defines all key terms, explains all abbreviations, provides comprehensive context, explores concepts in-depth, and presents information with exceptional clarity.

Please provide only the score (LOW, MEDIUM, or HIGH) without any additional justification.""",
            ),
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


class SourceValidateException(Exception):
    def __init__(self, resource_name: str, reason: str):
        super().__init__(f"Unable to validate source existence for \"{resource_name}\": {reason}")


def _validate_sources(sources: List[str]):
    """
    Validates the sources.

    Keyword arguments:
    sources -- The sources to validate
    """
    entries_tbl = EntriesClient()

    sources_tbl = SourcesClient()

    for source in sources:
        resource_name = OmniLakeResourceName.from_string(source)

        logging.debug(f"Validating source: {resource_name}")

        if resource_name.resource_type == "source":
            src = sources_tbl.get_by_source_id(source_id=resource_name.resource_id)

            if not src:
                raise SourceValidateException(
                    resource_name=source,
                    reason="Unable to locate source",
                )

        elif resource_name.resource_type == "entry":
            entry = entries_tbl.get(entry_id=resource_name.resource_id)

            if not entry:
                raise SourceValidateException(
                    resource_name=source,
                    reason="Unable to locate entry"
                )

        else:
            raise SourceValidateException(
                resource_name=source,
                reason="Unsupported resource type, only source and entry are supported sources",
            )


@fn_event_response(function_name='add_entry_processor', exception_reporter=ExceptionReporter(), logger=Logger("omnilake.ingestion.new_entry_processor"))
def handler(event: Dict, context: Dict):
    """
    Processes the new entries and adds them to the storage.
    """
    source_event = EventBusEvent.from_lambda_event(event)

    event_body = AddEntryBody(**source_event.body)

    jobs = JobsClient()

    job = jobs.get(job_type=event_body.job_type, job_id=event_body.job_id)

    # If the job has not started, set the start time
    if not job.started:
        job.started = datetime.now(tz=utc_tz)

    job.status = JobStatus.IN_PROGRESS

    source_validation_job = job.create_child(job_type='SOURCE_VALIDATION')

    jobs.put(job)

    # Cause the parent job to fail if the source validation fails
    with jobs.job_execution(job, failure_status_message='Failed to process entry',
                            skip_initialization=True, skip_completion=True):
        with jobs.job_execution(source_validation_job, failure_status_message='Failed to validate sources'):
            _validate_sources(event_body.sources)

        insight_extraction_job = job.create_child(job_type='EXTRACT_ENTRY_INSIGHTS')

        jobs.put(job)

        with jobs.job_execution(insight_extraction_job, failure_status_message='Failed to extract insights'):
            insights, invocation_resp = extract_insights(event_body.content)

            logging.debug(f"Invocation response: {invocation_resp}")

            insight_extraction_job.ai_statistics.invocations.append(invocation_resp.statistics)

            logging.debug(f"Extracted insights: {insights}")

            entry = Entry(
                analysis_scores={
                    'completeness': insights['completeness_score'],
                },
                char_count=len(event_body.content),
                content_hash=Entry.calculate_hash(event_body.content),
                effective_on=event_body.effective_on,
                original_of_source_id=event_body.original,
                sources=set(event_body.sources),
                tags=[tag.lower().strip() for tag in insights['tags'].split(',')],
            )

            entries = EntriesClient()

            entries.put(entry)

            storage_mgr = RawStorageManager()

            res = storage_mgr.save_entry(entry_id=entry.entry_id, content=event_body.content)

            logging.debug(f"Save entry result: {res}")

    if event_body.summarize:
        summarize_job = job.create_child(job_type='SUMMARIZE_ENTRY')

        jobs.put(job)

        with jobs.job_execution(summarize_job):
            summary_results = _summarize_content(event_body.content)

            event_publisher = EventPublisher()

            event_publisher.submit(
                event=EventBusEvent(
                    event_type=AddEntryBody.event_type,
                    body=AddEntryBody(
                        archive_id=event_body.archive_id,
                        content=summary_results.response,
                        effective_on=event_body.effective_on,
                        job_id=event_body.job_id,
                        immutable=True,
                        sources=[
                            str(EntryResourceName(entry.entry_id))
                        ]
                    ).to_dict()
                )
            )

    # If there is an archive ID, send an event to index the entry
    elif event_body.archive_id:
        event_publisher = EventPublisher()

        event_publisher.submit(
            event=EventBusEvent(
                event_type=IndexEntryBody.event_type,
                body=IndexEntryBody(
                    archive_id=event_body.archive_id,
                    entry_id=entry.entry_id,
                    job_id=event_body.job_id,
                    job_type=JobType.ADD_ENTRY,
                ).to_dict()
            ),
            delay=10 # Delay to give S3 time to catch up
        )