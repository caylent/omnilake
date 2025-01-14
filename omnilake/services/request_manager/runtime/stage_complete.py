"""
Handles the completion of a stage in the Lake Request lifecycle
"""
import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict, List

from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from da_vinci.exception_trap.client import ExceptionReporter

from omnilake.internal_lib.event_definitions import (
    LakeCompletionEventBodySchema,
    LakeRequestInternalRequestEventBodySchema,
    LakeRequestInternalResponseEventBodySchema,
)

from omnilake.tables.jobs.client import JobsClient, JobStatus
from omnilake.tables.lake_requests.client import (
    LakeRequest,
    LakeRequestsClient,
    LakeRequestStatus,
    LakeRequestStage
)
from omnilake.tables.registered_request_constructs.client import (
    RegisteredRequestConstructsClient,
    RequestConstructType,
)

CALLBACK_ON_FAILURE_EVENT_TYPE = "omnilake_request_internal_failure"


def _get_construct(operation_name: str, registered_construct_type: str, registered_construct_name: str) -> Dict:
    """
    Get a registered construct

    Keyword Arguments:
    operation_name -- The operation name
    registered_construct_type -- The type of the registered construct
    registered_construct_name -- The name of the registered construct
    """
    registered_constructs = RegisteredRequestConstructsClient()

    contruct_info = registered_constructs.get(
        registered_type_name=registered_construct_name,
        registered_construct_type=registered_construct_type,
    )

    if contruct_info is None:
        raise ValueError(f"Registered construct {registered_construct_name} not found")

    return {
        "event_type": contruct_info.get_operation_event_name(operation=operation_name),
        "schema": contruct_info.get_object_body_schema(operation=operation_name),
    }


def _close_out(lake_request: LakeRequest, lake_requests_client: LakeRequestsClient, entry_ids: List[str] = None,
               response_status: LakeRequestStatus = LakeRequestStatus.COMPLETED, status_message: str = None):
    """
    Close out the request and update the status.

    Keyword arguments:
    entry_ids -- The entry IDs
    lake_request -- The lake request
    lake_request_client -- The lake request client
    response_status -- The response status
    status_message -- The status message
    """
    logging.info(f"Request just finished final stage, marking as completed")

    lake_request.request_status = response_status

    lake_request.response_completed_on = datetime.now(tz=utc_tz)

    # If the request was completed, we should have the final entry ID
    if response_status == LakeRequestStatus.COMPLETED:
        lake_request.response_entry_id = entry_ids[0]

    lake_requests_client.put(lake_request)

    jobs_client = JobsClient()

    job = jobs_client.get(job_id=lake_request.job_id, job_type=lake_request.job_type)

    job.status = response_status

    job.ended = datetime.now(tz=utc_tz)

    if status_message:
        job.status_message = status_message

    jobs_client.put(job)

    event_publisher = EventPublisher()

    lake_request_finished_event = ObjectBody(
        body={
            "lake_request_id": lake_request.lake_request_id,
            "response_status": response_status,
        },
        schema=LakeCompletionEventBodySchema,
    )

    event_publisher.submit(event=EventBusEvent(
        body=lake_request_finished_event,
        event_type=lake_request_finished_event.get("event_type"),
    ))


_FN_NAME = 'omnilake.services.request_manager.lake_request_stage_complete'


def _send_next(entry_ids: List[str], lake_request_id: str, original_event: EventBusEvent, next_stage_body: ObjectBody,
               next_stage_event_name: str, parent_job_id: str, parent_job_type: str):
    """
    Send the next event for the next stage of the Lake Request's execution

    Keyword arguments:
    entry_ids -- The entry IDs
    lake_request_id -- The lake request ID
    original_event -- The original event, this is used to keep the EventBus chain going
    next_stage_body -- The next stage body
    next_stage_event_name -- The next stage event name
    parent_job_id -- The parent job ID
    parent_job_type -- The parent job type
    """
    logging.debug(f"Sending next stage event {next_stage_event_name}")

    next_stage_event = original_event.next_event(
        body=ObjectBody(
            body={
                "entry_ids": entry_ids,
                "lake_request_id": lake_request_id,
                "parent_job_id": parent_job_id,
                "parent_job_type": parent_job_type,
                "request_body": next_stage_body,
            },
            schema=LakeRequestInternalRequestEventBodySchema,
        ),
        callback_event_type=CALLBACK_ON_FAILURE_EVENT_TYPE,
        event_type=next_stage_event_name,
    )

    logging.debug(f"Submitting next stage event {next_stage_event.to_dict()}")

    event_publisher = EventPublisher()

    event_publisher.submit(event=next_stage_event)


@fn_event_response(function_name=_FN_NAME, exception_reporter=ExceptionReporter(), logger=Logger(_FN_NAME))
def handler(event, context):
    """
    Takes the response from a previous stage and updates the request status.
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=LakeRequestInternalResponseEventBodySchema,
    )

    lake_requests = LakeRequestsClient()

    lake_request = lake_requests.get(lake_request_id=event_body.get("lake_request_id"))

    if not lake_request:
        raise ValueError(f"Unable to locate lake request {event_body.get('lake_request_id')}")

    # Check Job for failure
    jobs_client = JobsClient()

    job = jobs_client.get(job_id=lake_request.job_id, job_type=lake_request.job_type)

    if job.status == JobStatus.FAILED:
        logging.error(f"Parent job {lake_request.job_id} failed, marking request as failed")

        _close_out(lake_request=lake_request, lake_requests_client=lake_requests, response_status=LakeRequestStatus.FAILED)

        return

    last_known_stage = lake_request.last_known_stage

    entry_ids = event_body["entry_ids"]

    ai_invocation_ids = event_body.get("ai_invocation_ids")

    # Update the request with the entry IDs
    if ai_invocation_ids:
        lake_requests.add_ai_invocation_ids(ai_invocation_ids=ai_invocation_ids, lake_request_id=lake_request.lake_request_id)

    # Kick off next Stage
    if last_known_stage == LakeRequestStage.LOOKUP:
        logging.info(f"Moving on to processing stage")

        # Moving on to processing
        lake_request.last_known_stage = LakeRequestStage.PROCESSING

        raw_object_body = lake_request.processing_instructions

        if len(entry_ids) < 1:
            logging.debug(f"No entries found for request {lake_request.lake_request_id} ... failing request")

            _close_out(
                lake_request=lake_request,
                lake_requests_client=lake_requests,
                response_status=LakeRequestStatus.FAILED,
                status_message="No entries found",
            )

            return

        construct_definition = _get_construct(
                operation_name='process',
                registered_construct_type=RequestConstructType.PROCESSOR,
                registered_construct_name=raw_object_body.get("processor_type"),
            )

        next_stage_body = ObjectBody(
            body=raw_object_body,
            schema=construct_definition["schema"],
        )

    elif last_known_stage == LakeRequestStage.PROCESSING:
        logging.info(f"Moving on to responding stage")

        # Moving on to responding
        lake_request.last_known_stage = LakeRequestStage.RESPONDING

        raw_object_body = lake_request.response_config

        if len(entry_ids) < 1:
            logging.debug(f"No entries found for request {lake_request.lake_request_id} ... failing request")

            _close_out(
                lake_request=lake_request,
                lake_requests_client=lake_requests,
                response_status=LakeRequestStatus.FAILED,
                status_message="No entries returned from processor",
            )

            return
        
        elif len(entry_ids) > 1:
            logging.debug(f"Too many entries found for request {lake_request.lake_request_id} ... failing request")

            _close_out(
                lake_request=lake_request,
                lake_requests_client=lake_requests,
                response_status=LakeRequestStatus.FAILED,
                status_message="Too many entries returned from processor",
            )

            return

        construct_definition = _get_construct(
            operation_name='respond',
            registered_construct_type=RequestConstructType.RESPONDER,
            registered_construct_name=raw_object_body.get("response_type"),
        )

        next_stage_body = ObjectBody(
            body=raw_object_body,
            schema=construct_definition["schema"],
        )

    elif last_known_stage == LakeRequestStage.RESPONDING:
        logging.info(f"Request is complete, closing out")

        _close_out(lake_request=lake_request, entry_ids=entry_ids, lake_requests_client=lake_requests)

        return

    else:
        raise ValueError(f"Unknown stage {last_known_stage}") 

    logging.info(f"Sending next stage event '{construct_definition['event_type']}'")

    _send_next(
        entry_ids=entry_ids,
        lake_request_id=lake_request.lake_request_id,
        original_event=source_event,
        next_stage_body=next_stage_body,
        next_stage_event_name=construct_definition["event_type"],
        parent_job_id=lake_request.job_id,
        parent_job_type=lake_request.job_type,
    )

    lake_requests.put(lake_request)