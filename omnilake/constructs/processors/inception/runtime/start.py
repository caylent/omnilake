import logging

from typing import Dict

from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.event_definitions import (
    LakeRequestInternalRequestEventBodySchema,
)

from omnilake.tables.jobs.client import JobsClient

from omnilake.constructs.processors.inception.runtime.manager import (
    ChainRequestManager,
    pseudo_transpiler,
    ReplacementDeclaration,
)


_FN_NAME = "omnilake.constructs.processors.chain.start"


@fn_event_response(exception_reporter=ExceptionReporter(), function_name=_FN_NAME,
                   logger=Logger(_FN_NAME), handle_callbacks=True)
def handler(event: Dict, context: Dict):
    '''
    Kicks off the chain by replacing the pseudo parameters with the actual values
    and saving to the inception table
    '''
    logging.debug(f'Received request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=LakeRequestInternalRequestEventBodySchema,
    )

    omni_jobs = JobsClient()

    job = omni_jobs.get(job_id=event_body.get("parent_job_id"), job_type=event_body.get("parent_job_type"),
                        consistent_read=True)

    child_job = job.create_child(job_type="LAKE_PROCESSOR_CHAIN")

    with omni_jobs.job_execution(job=child_job, skip_completion=True):

        req_body = event_body["request_body"]

        chain_definition = req_body["chain_definition"]

        entry_distribution_mode = req_body.get("entry_distribution_mode")

        req_mgr = ChainRequestManager(jobs_client=omni_jobs)

        standard_replacements = [
            ReplacementDeclaration(
                lake_request_stage_name='response_config',
                max_declarations=1,
                min_declarations=1,
                replacement_value={"response_type": "DIRECT"},
                type_name='EXPORT_RESPONSE',
            ),
        ]

        join_instructions = req_body.get("join_instructions")

        if entry_distribution_mode == "INDIVIDUAL":
            if len(event_body["entry_ids"]) > 1 and not join_instructions:
                raise ValueError(
                    "Individual entry distribution mode requires join instructions when more than one entry is provided"
                )

            for entry_id in event_body["entry_ids"]:
                logging.debug(f"Creating chain for: {entry_id}")

                individual_replacement = [
                    ReplacementDeclaration(
                        lake_request_stage_name='lookup_instructions',
                        replacement_value={
                            "entry_id": entry_id,
                            "request_type": "DIRECT_ENTRY",
                        },
                        type_name='PARENT_CHAIN_ENTRIES'
                    )
                ]

                individual_replacement.extend(standard_replacements)

                full_chain_definition = pseudo_transpiler(
                    chain_definition=chain_definition,
                    replacements=individual_replacement,
                )

                req_mgr.submit_chain_request(
                    callback_event_type='omnilake_processor_inception_chain_complete',
                    lake_request_id=event_body['lake_request_id'],
                    parent_job=child_job,
                    request=full_chain_definition,
                )

        else:
            standard_replacements.append(
                ReplacementDeclaration(
                    lake_request_stage_name='lookup_instructions',
                    replacement_value={
                        "entry_ids": event_body["entry_ids"],
                        "request_type": "BULK_ENTRY",
                    },
                    type_name='PARENT_CHAIN_ENTRIES'
                )
            )

            full_chain_definition = pseudo_transpiler(
                chain_definition=chain_definition,
                replacements=standard_replacements,
            )

            req_mgr.submit_chain_request(
                lake_request_id=event_body['lake_request_id'],
                callback_event_type='omnilake_processor_inception_chain_complete',
                parent_job=child_job,
                request=full_chain_definition,
            )