'''
Handle final responses
'''
import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict
from uuid import uuid4

from da_vinci.core.global_settings import setting_value
from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.ai import AI
from omnilake.internal_lib.clients import RawStorageManager
from omnilake.internal_lib.event_definitions import (
    IndexEntryEventBodySchema,
    LakeRequestInternalResponseEventBodySchema,
    LakeRequestInternalRequestEventBodySchema,
)

from omnilake.internal_lib.clients import AIStatisticSchema, AIStatisticsCollector
from omnilake.internal_lib.naming import EntryResourceName

from omnilake.tables.entries.client import EntriesClient
from omnilake.tables.jobs.client import JobsClient
from omnilake.tables.provisioned_archives.client import ArchivesClient
from omnilake.tables.registered_request_constructs.client import (
    RegisteredRequestConstructsClient,
    RequestConstructType,
)


def _get_index_endpoint(archive_id: str) -> str:
    """
    Gets the index endpoint for the archive.

    Keyword arguments:
    archive_id -- The archive ID
    """
    archives = ArchivesClient()

    archive = archives.get(archive_id=archive_id)

    if not archive:
        raise ValueError(f"Unable to locate archive {archive_id}")

    archive_type = archive.archive_type

    registered_constructs = RegisteredRequestConstructsClient()

    registered_construct = registered_constructs.get(
        registered_construct_type=RequestConstructType.ARCHIVE,
        registered_type_name=archive_type,
    )

    if not registered_construct:
        raise ValueError(f"No registered construct for archive type {archive_id}")

    return registered_construct.get_operation_event_name(operation="index")


class ResponsePrompt:
    """
    Response prompt
    """
    def __init__(self, goal: str, entry_id: str):
        self.goal = goal

        self._entry_id = entry_id

        self._sources_client = None

        self._entries_client = EntriesClient()

        self._storage_manager = RawStorageManager()

    def _get_content(self, entry_id: str) -> str:
        '''
        Gets the content

        Keyword arguments:
        entry_id -- The entry ID
        '''
        content_resp = self._storage_manager.get_entry(entry_id=entry_id)

        if content_resp.status_code >= 400:
            raise ValueError(f"Entry content for ID {entry_id} could not be retrieved.")

        content = content_resp.response_body.get('content')

        if not content:
            raise ValueError(f"Entry with ID {entry_id} is empty.")

        return content

    def generate(self):
        '''
        Generates the response prompt.
        '''
        content = self._get_content(self._entry_id)

        prompt = setting_value(namespace='omnilake::simple_responder', setting_key='default_response_prompt')

        full_prompt_lst = [
            prompt,
            f"\n\nUSER GOAL:\n\n{self.goal}",
            f"\n\nCONTENT TO USE FOR RESPONSE:\n\n{content}"
        ]

        return "\n\n".join(full_prompt_lst)

    def to_str(self):
        '''
        Converts the response prompt to a string.
        '''
        return self.generate()


_FN_NAME = "omnilake.constructs.responders.simple.response"


@fn_event_response(exception_reporter=ExceptionReporter(), logger=Logger(_FN_NAME),
                   function_name=_FN_NAME, handle_callbacks=True)
def final_responder(event: Dict, context: Dict) -> None:
    """
    Final responder function
    """
    logging.debug(f'Received request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=LakeRequestInternalRequestEventBodySchema,
    )

    jobs = JobsClient()

    parent_job = jobs.get(job_type=event_body.get("parent_job_type"), job_id=event_body.get("parent_job_id"))

    final_resp_job = parent_job.create_child(job_type="CONSTRUCT_RESPONDER_SIMPLE_FINAL_RESPONSE")

    jobs.put(parent_job)

    job_failure_message = 'Failed to process final response'

    with jobs.job_execution(final_resp_job, failure_status_message=job_failure_message):

        logging.debug(f'Processing final response: {event_body}')

        response_config = event_body["request_body"]

        goal = response_config["goal"]

        entries = event_body["entry_ids"]

        if len(entries) != 1:
            raise ValueError("Only one entry is supported by the SMIPLE responder")

        entry_id = entries[0]

        final_response_prompt = ResponsePrompt(
            goal=goal,
            entry_id=entry_id
        )

        prompt = final_response_prompt.to_str()

        logging.debug(f'Final response prompt: {prompt}')

        ai = AI()

        model_id = response_config.get("model_id")

        final_response = ai.invoke(prompt=prompt, max_tokens=8000, model_id=model_id)

        logging.debug(f'Response result: {final_response}')

        raw_storage = RawStorageManager()

        resp = raw_storage.create_entry(
            content=final_response.response,
            effective_on=datetime.now(tz=utc_tz).isoformat(),
            sources=[str(EntryResourceName(entry_id))]
        )

        entry_id = resp.response_body["entry_id"]

        logging.debug(f'Raw storage response: {resp}')

        stats_collector = AIStatisticsCollector()

        invocation_id = str(uuid4())

        ai_statistic = ObjectBody(
            body={
                "invocation_id": invocation_id,
                "job_type": parent_job.job_type,
                "job_id": parent_job.job_id,
                "model_id": final_response.statistics.model_id,
                "resulting_entry_id": entry_id,
                "total_output_tokens": final_response.statistics.output_tokens,
                "total_input_tokens": final_response.statistics.input_tokens,
            },
            schema=AIStatisticSchema,
        )

        stats_collector.publish(statistic=ai_statistic)

        logging.debug(f'AI Response: {final_response.response}')

        event_publisher = EventPublisher()

        publish = ObjectBody(
            body={
                "lake_request_id": event_body.get("lake_request_id"),
                "entry_ids": [entry_id],
                "ai_invocation_ids": [invocation_id],
            },
            schema=LakeRequestInternalResponseEventBodySchema,
        )

        event_publisher.submit(
            event=source_event.next_event(
                event_type=publish.get("event_type"),
                body=publish.to_dict()
            ),
        )

    logging.debug(f'Final response job completed: {final_resp_job.job_id}')

    destination_archive_id = response_config.get("destination_archive_id")

    # Index the entry if a destination archive ID is provided
    if destination_archive_id:
        index_body = ObjectBody(
            body={
                "archive_id": destination_archive_id,
                "entry_id": entry_id,
                "parent_job_id": parent_job.job_id,
                "parent_job_type": parent_job.job_type,
            },
            schema=IndexEntryEventBodySchema,
        )

        logging.debug(f"Indexing entry {entry_id} for archive {destination_archive_id}: {index_body.to_dict()}")

        event_type = _get_index_endpoint(archive_id=destination_archive_id)  

        event_publisher.submit(
            event=source_event.next_event(
                event_type=event_type,
                body=index_body.to_dict()
            ),
            delay=5 # Delay to give S3 time to catch up
        )