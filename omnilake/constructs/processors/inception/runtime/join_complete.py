"""
Join completion handler for the inception processor
"""
import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict

from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.event_definitions import (
    LakeChainCompletionEventBodySchema,
    LakeRequestInternalResponseEventBodySchema,
)

from omnilake.internal_lib.exceptions import CALLBACK_ON_FAILURE_EVENT_TYPE

from omnilake.tables.jobs.client import JobsClient, JobStatus

from omnilake.tables.lake_requests.client import LakeRequestsClient

from omnilake.tables.lake_chain_requests.client import LakeChainRequestsClient

from omnilake.constructs.processors.inception.tables.chain_inception_runs.client import (
    ChainInceptionRunClient,
)

from omnilake.constructs.processors.inception.runtime.manager import (
    get_inception_job,
    lake_chain_failure_reason,
)


_FN_NAME = "omnilake.constructs.processors.chain.join_completion"


@fn_event_response(exception_reporter=ExceptionReporter(), function_name=_FN_NAME,
                   logger=Logger(_FN_NAME), handle_callbacks=True)
def handler(event: Dict, context: Dict):
    '''
    Handles the completion of an output joiner chain
    '''
    logging.debug(f'Received request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=LakeChainCompletionEventBodySchema,
    )

    chain_request_id = event_body["chain_request_id"]

    response_status = event_body["response_status"]

    event_bus = EventPublisher()

    # Get the corresponding Lake Request
    chain_inception_runs = ChainInceptionRunClient()

    chain_inception_run = chain_inception_runs.get_by_chain_request_id(chain_request_id=chain_request_id)

    chain_inception_run.execution_status = response_status

    chain_inception_runs.put(chain_inception_run)

    chains = LakeChainRequestsClient()

    chain_request = chains.get(chain_request_id=chain_request_id)

    if not chain_request:
        raise ValueError(f"Chain Request not found for chain request id {chain_request_id}")

    # Handle the case where the chain execution failed
    if response_status == "FAILED":
        processor_job = get_inception_job(lake_request_id=chain_inception_run.lake_request_id)

        processor_job.ended = datetime.now(tz=utc_tz)

        processor_job.status = JobStatus.FAILED

        failed_message = "Chain execution failed"

        failure_reason = lake_chain_failure_reason(chain_request_id=chain_request_id)

        if failure_reason:
            failed_message = f"{failed_message}: {failure_reason}"

        processor_job.status_message = failed_message

        omni_jobs = JobsClient()

        omni_jobs.put(processor_job)

        failure_event_body = ObjectBody(
            body={
                "originating_event_details": {
                    "event_body": {"lake_request_id": chain_inception_run.lake_request_id},
                },
                "da_vinci_event_bus_response": {
                    "reason": failed_message,
                }
            }
        )

        # Send Event failure to the lake
        event_bus.submit(
            event=source_event.next_event(
                body=failure_event_body.to_dict(),
                event_type=CALLBACK_ON_FAILURE_EVENT_TYPE,
            ),
        )

        return

    # Find the entry
    lake_requests = LakeRequestsClient()

    response_lake_request = lake_requests.get(lake_request_id=chain_request.executed_requests['inception_joiner'])

    final_body = ObjectBody(
        body={
            "entry_ids": [response_lake_request.response_entry_id],
            "lake_request_id": chain_inception_run.lake_request_id,
        },
        schema=LakeRequestInternalResponseEventBodySchema,
    )

    event_bus.submit(
        event=source_event.next_event(
            body=final_body.to_dict(),
            event_type=final_body["event_type"],
        )
    )

    process_job = get_inception_job(lake_request_id=chain_inception_run.lake_request_id)

    process_job.status = JobStatus.COMPLETED

    process_job.completed = datetime.now(tz=utc_tz)

    omni_jobs = JobsClient()

    omni_jobs.put(process_job)