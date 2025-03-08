"""
Handles the complettion of the initial chain(s)
"""
import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict, List

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

from omnilake.tables.lake_requests.client import LakeRequest, LakeRequestsClient

from omnilake.tables.lake_chain_requests.client import (
    LakeChainRequestsClient,
)

from omnilake.constructs.processors.inception.tables.chain_inception_runs.client import (
    InceptionExecutionStatus,
    ChainInceptionRunClient,
)

from omnilake.constructs.processors.inception.tables.inception_mutex.client import (
    InceptionMutexClient,
)

from omnilake.constructs.processors.inception.runtime.manager import (
    ChainRequestManager,
    get_inception_job,
    lake_chain_failure_reason,
)


def check_processor_complete(lake_request: LakeRequest):
    """
    Checks if the processor is complete by checking the lake request table
    """
    original_processing_instructions = lake_request.processing_instructions

    distribution_mode = original_processing_instructions.get("entry_distribution_mode")

    # If the distribution mode is ALL, then only one response is expected, so if this
    # is being called, then the processor is complete
    if distribution_mode == "ALL":
        return True

    # distribution_mode is "INDIVIDUAL"
    chain_inception_runs = ChainInceptionRunClient()

    all_runs = chain_inception_runs.all_by_lake_request_id(lake_request_id=lake_request.lake_request_id)

    all_run_chain_ids = [run.chain_request_id for run in all_runs]

    completed_statuses = [InceptionExecutionStatus.COMPLETED, InceptionExecutionStatus.FAILED]

    all_completed_runs = [run.chain_request_id for run in all_runs if run.execution_status in completed_statuses]

    return len(all_run_chain_ids) == len(all_completed_runs)


def _identify_responding_request_id(chain_definition: List[Dict], chain_id: str) -> str:
    """
    Identifies the responding request id based on the processing instructions and the chain id

    Keyword arguments:
    chain_definition -- The chain definition that is being processed
    chain_id -- The chain id that is being processed
    """
    lake_request_name = None

    for lake_request in chain_definition:
        if lake_request["lake_request"]["response_config"]["response_type"] == "EXPORT_RESPONSE":
            lake_request_name = lake_request["name"]

            break

    if not lake_request_name:
        raise ValueError("No lake request found with 'EXPORT_RESPONSE' responder type")

    lake_chain_requests = LakeChainRequestsClient()

    chain = lake_chain_requests.get(chain_request_id=chain_id)

    return chain.executed_requests[lake_request_name]


def get_response_entry_ids(lake_request: LakeRequest) -> List[str]:
    """
    Gets the entry ids that are expected to be returned by this chain

    Keyword arguments:
    lake_request -- The lake request that is being processed
    chain_id -- The chain id that is being processed
    """
    original_processing_instructions = lake_request.processing_instructions

    lake_requests = LakeRequestsClient()

    response_entry_ids = []

    chain_inception_runs = ChainInceptionRunClient()

    all_runs = chain_inception_runs.all_by_lake_request_id(lake_request_id=lake_request.lake_request_id)

    all_run_chain_ids = [run.chain_request_id for run in all_runs]

    for chain_id in all_run_chain_ids:
        responding_request_id = _identify_responding_request_id(
            chain_definition=original_processing_instructions["chain_definition"],
            chain_id=chain_id,
        )

        responding_lake_request = lake_requests.get(lake_request_id=responding_request_id, consistent_read=True)

        response_entry = responding_lake_request.response_entry_id

        if not response_entry:
            raise ValueError(f"Response entry id not found for lake request {responding_request_id}")

        response_entry_ids.append(response_entry)

    return response_entry_ids


def submit_join_request(originating_lake_request_id: str, entry_ids: List[str], join_instructions: Dict, lock_id: str):
    """
    Submits a join request to join the results of the chains together

    Keyword arguments:
    originating_lake_request_id -- The ID of the originating lake request
    lake_request -- The lake request that is being processed
    entry_ids -- The entry ids that are being joined
    join_instructions -- The instructions for joining the entries
    """
    chain = [
        {
            "name": "inception_joiner",
            "lake_request": {
                "lookup_instructions": [
                    {
                        "entry_ids": entry_ids,
                        "request_type": "BULK_ENTRY"
                    }
                ],
                "processing_instructions": join_instructions,
                "response_config": {
                    "response_type": "DIRECT",
                },
            },
        }
    ]

    logging.debug(f"Constructed final joiner chain: {chain}")

    chain_manager = ChainRequestManager(jobs_client=JobsClient())

    chain_manager.submit_chain_request(
        lake_request_id=originating_lake_request_id,
        callback_event_type='omnilake_processor_inception_join_completion',
        parent_job=get_inception_job(lake_request_id=originating_lake_request_id),
        request=chain,
        validate_lock_id=lock_id,
    )


_FN_NAME = "omnilake.constructs.processors.chain.completion"


@fn_event_response(exception_reporter=ExceptionReporter(), function_name=_FN_NAME,
                   logger=Logger(_FN_NAME), handle_callbacks=True)
def handler(event: Dict, context: Dict):
    '''
    Handles the completion of the initial chain(s)
    '''
    logging.debug(f'Received request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=LakeChainCompletionEventBodySchema,
    )

    chain_request_id = event_body["chain_request_id"]

    response_status = event_body["response_status"]

    # Get the corresponding Lake Request
    chain_inception_runs = ChainInceptionRunClient()

    chain_inception_run = chain_inception_runs.get_by_chain_request_id(chain_request_id=chain_request_id)

    if not chain_inception_run:
        raise ValueError(f"Chain Inception Run not found for chain request id {chain_request_id}")

    lake_requests = LakeRequestsClient()

    lake_request = lake_requests.get(lake_request_id=chain_inception_run.lake_request_id)

    if not lake_request:
        raise ValueError(f"Lake Request not found for lake request id {chain_inception_run.lake_request_id}")

    chain_inception_run.execution_status = response_status

    chain_inception_runs.put(chain_inception_run)

    # If the Lake request was already failed due to another failure, then we don't need to do anything
    if lake_request.request_status == JobStatus.FAILED:
        logging.info(f"Lake request {lake_request.lake_request_id} already failed ... nothing to do")

        return

    event_bus = EventPublisher()

    # COMPLETED or FAILED
    if response_status == "FAILED":
        processor_job = get_inception_job(lake_request_id=lake_request.lake_request_id)

        processor_job.ended = datetime.now(tz=utc_tz)

        processor_job.status = JobStatus.FAILED

        failure_message = "Chain execution failed"
        
        failure_reason = lake_chain_failure_reason(chain_request_id=chain_request_id)

        if failure_reason:
            failure_message = f"{failure_message}: {failure_reason}"

        processor_job.status_message = failure_message

        omni_jobs = JobsClient()

        omni_jobs.put(processor_job)

        failure_event_body = ObjectBody(
            body={
                "originating_event_details": {
                    "event_body": {"lake_request_id": lake_request.lake_request_id},
                },
                "da_vinci_event_bus_response": {
                    "reason": failure_message,
                }
            }
        )

        # Send Event failure to the lake
        publisher = EventPublisher()

        publisher.submit(
            event=source_event.next_event(
                body=failure_event_body.to_dict(),
                event_type=CALLBACK_ON_FAILURE_EVENT_TYPE,
            ),
        )

        return

    if check_processor_complete(lake_request=lake_request):
        # If completed and it was more than one chain, then construct and launch the next chain
        # to join the results from each chain together
        resulting_entry_ids = get_response_entry_ids(lake_request=lake_request)

        if len(resulting_entry_ids) == 1:
            # Respond back to the lake request with the entry id
            final_body = ObjectBody(
                body={
                    "entry_ids": resulting_entry_ids,
                    "lake_request_id": lake_request.lake_request_id,
                },
                schema=LakeRequestInternalResponseEventBodySchema,
            )

            event_bus.submit(
                event=source_event.next_event(
                    body=final_body.to_dict(),
                    event_type=final_body["event_type"],
                )
            )

            logging.info(f"Responded to lake request {lake_request.lake_request_id}")

            return

        # Grab lock to prevent multiple join requests from being submitted
        mutex_client = InceptionMutexClient()

        lock_acquired = mutex_client.request_lock(lake_request_id=lake_request.lake_request_id)

        if not lock_acquired:
            logging.info(f"Another join request is already in progress for lake request {lake_request.lake_request_id}")

            return

        join_instructions = lake_request.processing_instructions.get("join_instructions")

        submit_join_request(
            originating_lake_request_id=lake_request.lake_request_id,
            entry_ids=resulting_entry_ids,
            join_instructions=join_instructions,
            lock_id=lock_acquired,
        )

        return