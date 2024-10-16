import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict

from da_vinci.core.global_settings import setting_value
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.ai import AI, ModelIDs
from omnilake.internal_lib.ai_insights import (
    AIResponseDefinition,
    AIResponseInsightDefinition,
    ResponseParser,
)
from omnilake.internal_lib.event_definitions import (
    QueryRequestBody,
    VSQueryBody,
)

from omnilake.tables.jobs.client import Job, JobsClient, JobStatus
from omnilake.tables.vector_store_queries.client import VectorStoreQuery, VectorStoreQueryClient

from omnilake.services.storage.runtime.vector_storage import choose_vector_stores


@fn_event_response(exception_reporter=ExceptionReporter(), function_name='query_request',
                   logger=Logger('omnilake.services.storage.query_request'))
def handler(event: Dict, context: Dict):
    """
    Handle the query request

    Keyword arguments:
    event -- The event that triggered the function.
    context -- The context of the function.
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = QueryRequestBody(**source_event.body)

    jobs = JobsClient()

    if event_body.parent_job_type and event_body.parent_job_id:

        parent_job = jobs.get(job_type=event_body.parent_job_type, job_id=event_body.parent_job_id)

        query_job = parent_job.create_child(job_type='QUERY_REQUEST')

        jobs.put(parent_job)

    else:
        query_job = Job(job_type='QUERY_REQUEST')

    query_job.status = JobStatus.IN_PROGRESS

    query_job.started = datetime.now(tz=utc_tz)

    jobs.put(query_job)

    response_definition = AIResponseDefinition(
        insights=[
            AIResponseInsightDefinition(
                name="tags",
                definition="Provide a list of tags that should be used to find the appropriate vector stores to query.\n\n- These tags should be based on the user's request.\n\n- Provide tags as a comma-separated list",
            )
        ]
    )

    prompt = response_definition.to_prompt(event_body.query_string)

    ai = AI()

    result = ai.invoke(
        model_id=ModelIDs.HAIKU,
        prompt=prompt,
    )

    logging.debug(f'AI result: {result}')

    parser = ResponseParser()

    parser.feed(result.response)

    parsed_insights = parser.parsed_insights()

    logging.debug(f'Parsed insights: {parsed_insights}')

    vs_tags = [tag.lower().strip() for tag in parsed_insights['tags'].split(',')]

    logging.debug(f'Searching for vector store based on following tags: {vs_tags}')

    vector_store_search_list = choose_vector_stores(archive_id=event_body.archive_id, expected_tags=vs_tags)

    logging.debug(f'Found vector stores: {vector_store_search_list}')

    max_vector_store_search_group_size = setting_value(namespace='storage', setting_key='max_vector_store_search_group_size')

    grouper_fn = lambda lst, n: [lst[i:i + n] for i in range(0, len(lst), n)]

    vector_store_search_groups = grouper_fn(vector_store_search_list, max_vector_store_search_group_size)

    vector_store_query = VectorStoreQuery(
        archive_id=event_body.archive_id,
        job_id=query_job.job_id,
        job_type='QUERY_REQUEST',
        max_entries=event_body.max_entries,
        remaining_processes=len(vector_store_search_groups),
        request_id=event_body.request_id,
        query=event_body.query_string,
        target_tags=vs_tags,
        vector_store_ids=vector_store_search_list,
    )

    logging.debug(f'Saving vector store query information: {vector_store_query}')

    vector_store_query_client = VectorStoreQueryClient()

    vector_store_query_client.put(vector_store_query)

    event_publisher = EventPublisher()

    for vector_store_search_group in vector_store_search_groups:
        vs_query_body = VSQueryBody(
            archive_id=event_body.archive_id,
            query_id=vector_store_query.query_id,
            query_str=event_body.query_string,
            vector_store_ids=vector_store_search_group,
        )

        logging.debug(f'Submitting event: {vs_query_body}')

        event_publisher.submit(
            event=EventBusEvent(
                body=vs_query_body.to_dict(),
                event_type=VSQueryBody.event_type,
            )
        )