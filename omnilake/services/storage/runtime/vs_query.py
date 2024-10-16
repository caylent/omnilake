"""
Handles the Vector Storage queries
"""

import json
import logging

from dataclasses import dataclass, field
from typing import Dict, List

import asyncio
import boto3
import lancedb

from da_vinci.core.global_settings import setting_value
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.event_definitions import (
    QueryCompleteBody,
    VSQueryBody,
)
from omnilake.internal_lib.naming import EntryResourceName

from omnilake.tables.jobs.client import JobsClient


@dataclass
class AsyncResponse:
    """
    Asynchronous response class.
    """
    results: List[str]
    missing: List[str] = field(default_factory=list)


async def _async_load_result(db: lancedb.DBConnection, vector_store_name: str, query: str, result_limits: int,
                             use_ft_search: bool = False):
    """
    Asyn load the query from the Vector Storage service. Do not call this function directly.

    Keyword arguments:
    db -- The connection to the Vector Storage service
    vector_store_name -- The name of the vector store to query
    query -- The query to perform
    result_limits -- The number of results to return
    use_ft_search -- Whether to use full text search
    """
    table = db.open_table(name=vector_store_name)

    try:
        if use_ft_search:
            table.create_fts_index('text')

        result = table.search(query).metric("cosine").limit(result_limits).to_list()
    except Exception as e:
        logging.error(f'Error querying vector store {vector_store_name}: {e}')

        return None

    context = [r["entry_id"] for r in result]

    return context


async def async_load_results(db: lancedb.DBConnection, vector_store_names: List[str], prompt: str,
                             result_limits_per_store: int = 100, use_ft_search: bool = False):
    """
    Async load the results from the Vector Storage service.

    Keyword arguments:
    db -- The connection to the Vector Storage service
    vector_store_names -- The names of the vector stores to query
    prompt -- The prompt to query
    result_limits_per_kb -- The number of results to return per KB
    use_ft_search -- Whether to use full text search
    """
    tasks = []

    for vs in vector_store_names:
         task = asyncio.create_task(_async_load_result(db, vs, prompt, result_limits_per_store, use_ft_search))

         tasks.append(task)

    results = await asyncio.gather(*tasks)

    flattened_results = []

    missing_vs = []

    logging.debug(f'Query results: {results}')

    for result in results:
        if result is None:
            missing_vs.append(vector_store_names[results.index(result)])

            continue

        flattened_results.extend(result)

    logging.debug(f'Flattened results: {flattened_results}')

    return AsyncResponse(
        results=flattened_results,
        missing=missing_vs
    )


class VectorStorageSearch:
    """
    Vector Storage Query
    """
    @staticmethod
    def text_embedding(text: str):
        """
        Create a prompt embedding for the query.

        Keyword Arguments:
            prompt: The prompt to query
        """
        bedrock = boto3.client(service_name='bedrock-runtime')
    
        body = json.dumps({
            "inputText": text
        })
        
        response = bedrock.invoke_model(
            modelId="amazon.titan-embed-text-v2:0",
            contentType="application/json",
            accept="application/json",
            body=body
        )
        
        response_body = json.loads(response['body'].read())

        embedding = response_body['embedding']
        
        return embedding

    def query(self, vector_store_names: List[str], content: str, result_limits_per_kb: int = 30, vector_query: bool = True):
        """
        Entry point for the query API Lambda function.
        """
        storage_bucket_name = setting_value(namespace='storage', setting_key='vector_store_bucket')

        db = lancedb.connect(f's3://{storage_bucket_name}')

        use_ft_search = False

        if vector_query:
            query = self.text_embedding(content)

        else:
            query = content

            use_ft_search = True

        logging.info(f'Querying vector storage: {vector_store_names} with: {query}')

        return asyncio.run(async_load_results(db, vector_store_names, query, result_limits_per_kb, use_ft_search))


@fn_event_response(exception_reporter=ExceptionReporter(), logger=Logger("omnilake.services.storage.vector_storage_query"),
                   function_name="vector_storage_query")
def handler(event: Dict, context: Dict):
    """
    Entry point for the query API Lambda function.
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = VSQueryBody(**source_event.body)

    jobs = JobsClient()

    parent_job = jobs.get(job_type=event_body.parent_job_type, job_id=event_body.parent_job_id)

    query_job = parent_job.create_child(job_type='VECTOR_STORAGE_QUERY')

    jobs.put(parent_job)

    with jobs.job_execution(query_job, fail_all_parents=True, failure_status_message='Failed to execute vector storage query'):
        vector_storage_search = VectorStorageSearch()

        vector_store_names = [f'{event_body.archive_id}-{vs_id}' for vs_id in event_body.vector_store_ids]

        query_result = vector_storage_search.query(vector_store_names=vector_store_names, content=event_body.query_str)

        logging.debug(f'Query result: {query_result}')

        de_duplicated_results = list(set(query_result.results))

        # TODO: Implement sorting if max_entries < result count

        resource_names = [str(EntryResourceName(resource_id=entry_id)) for entry_id in de_duplicated_results]

        logging.debug(f'Resource names: {resource_names}')

        event_publisher = EventPublisher()

        event_publisher.submit(
            event=EventBusEvent(
                body=QueryCompleteBody(
                    query_id=event_body.query_id,
                    resource_names=resource_names,
                ).to_dict(),
                event_type=QueryCompleteBody.event_type
            )
        )