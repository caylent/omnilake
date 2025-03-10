"""
Library for managing the chain requests
"""
import logging

from copy import deepcopy
from dataclasses import dataclass
from typing import Dict, List, Union
from uuid import uuid4

from da_vinci.core.immutable_object import ObjectBody

from da_vinci.event_bus.client import EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.event_definitions import (
    LakeChainRequestEventBodySchema,
)

from omnilake.tables.jobs.client import Job, JobsClient

from omnilake.tables.lake_chain_requests.client import (
    LakeChainRequest,
    LakeChainRequestsClient,
)

from omnilake.constructs.processors.inception.tables.chain_inception_runs.client import (
    ChainInceptionRun,
    ChainInceptionRunClient,
)

from omnilake.constructs.processors.inception.tables.inception_mutex.client import (
    InceptionMutexClient,
)


def lake_chain_failure_reason(chain_request_id: str) -> Union[str, None]:
    """
    Determines the reason for the lake chain failure

    Keyword arguments:
    lake_chain_request -- The lake chain request object
    """
    lake_chain_requests = LakeChainRequestsClient()

    lake_chain_request = lake_chain_requests.get(chain_request_id=chain_request_id)

    # Grab the job from the chain 
    omni_jobs = JobsClient()

    job = omni_jobs.get(job_id=lake_chain_request.job_id, job_type=lake_chain_request.job_type)

    if job.status == 'FAILED':
        return job.status_message

    return None


def get_inception_job(lake_request_id: str) -> Job:
    """
    Gets the inception job for the lake request

    Keyword arguments:
    lake_request -- The lake request the executing processor belongs to
    """
    # Get the job out of the chain inception run
    inception_run_client = ChainInceptionRunClient()

    inception_run = inception_run_client.all_by_lake_request_id(lake_request_id=lake_request_id)

    if not inception_run:
        raise ValueError(f"Could not find any chain inception runs for lake request {lake_request_id}")

    sampled_run = inception_run[0]

    omni_jobs = JobsClient()

    return omni_jobs.get(job_id=sampled_run.job_id, job_type=sampled_run.job_type)


@dataclass
class ReplacementDeclaration:
    """
    Simple declaration for Pseudo types

    Taken by the pseudo_transpiler function
    """
    lake_request_stage_name: str
    replacement_value: Dict
    type_name: str
    max_declarations: int = None
    min_declarations: int = None


def pseudo_transpiler(chain_definition: List[Dict], replacements: List[ReplacementDeclaration]) -> Dict:
    """
    Replaces the pseudo parameters with the actual values

    Keyword arguments:
    chain_definition -- List of dictionaries that define the chain
    replacements -- List of ReplacementDeclaration objects
    """
    normalized_definition = []

    for request in chain_definition:
        if isinstance(request, ObjectBody):
            normalized_definition.append(request.to_dict())

        else:
            normalized_definition.append(request)

    logging.debug(f"compiling chain_definition: {normalized_definition}")

    new_chain_definition = deepcopy(normalized_definition)

    for replacement in replacements:
        declarations = 0

        for idx, request in enumerate(new_chain_definition):

            request_config = request['lake_request']

            logging.debug(f"request: {request}")

            if replacement.lake_request_stage_name == 'lookup_instructions':
                # Handle lookup_instructions case

                if replacement.lake_request_stage_name in request_config:
                    for lookup_idx, lookup_instruction in enumerate(request['lake_request'][replacement.lake_request_stage_name]):

                        # I know the origin of this lookup type name but it still hurts me lol ... :facepalm: Jim
                        if lookup_instruction['request_type'] == replacement.type_name:
                            declarations += 1

                            new_chain_definition[idx]['lake_request'][replacement.lake_request_stage_name][lookup_idx] = replacement.replacement_value
            else:
                # Handle processor and response
                search_type_name = 'processor_type'

                if replacement.lake_request_stage_name == 'response_config':
                    search_type_name = 'response_type'

                logging.debug(f"search_type_name: {search_type_name}")
                
                if request_config[replacement.lake_request_stage_name][search_type_name] == replacement.type_name:
                    declarations += 1

                    new_chain_definition[idx]['lake_request'][replacement.lake_request_stage_name] = replacement.replacement_value

            if replacement.max_declarations and declarations > replacement.max_declarations:
                raise ValueError(f"Too many declarations for {replacement.type_name}")

        if replacement.min_declarations and declarations < replacement.min_declarations:
            raise ValueError(f"Chain missing required declaration {replacement.type_name}") 
    
    return new_chain_definition


class ChainRequestManager:
    """
    Manages the creation of chain requests
    """

    def __init__(self, jobs_client: JobsClient):
        """
        Initialize the ChainRequestManager object

        Keyword arguments:
        jobs_client -- JobsClient object
        """
        self.jobs_client = jobs_client

        self.chain_inception_run = ChainInceptionRunClient()

        self.event_publisher = EventPublisher()

        self.lake_chain_requests = LakeChainRequestsClient()

    def submit_chain_request(self, lake_request_id: str, parent_job: Job, request: List[Dict],
                             callback_event_type: str = 'omnilake_processor_inception_chain_complete',
                             validate_lock_id: str = None) -> None:
        """
        Submits a lake chain request

        Keyword arguments:
        lake_request_id -- The ID of the lake request
        parent_job -- The parent job object
        request -- The chain request
        callback_event_type -- The event type to call when the chain is complete
        validate_lock_id -- The lock ID to validate
        """
        job = parent_job.create_child(job_type='LAKE_CHAIN_REQUEST')

        self.jobs_client.put(job)

        normalized_chain = []

        for req in request:
            if isinstance(req, ObjectBody):
                normalized_chain.append(req.to_dict())

            else:
                normalized_chain.append(req)

        logging.debug(f"normalized_chain: {normalized_chain}")

        chain_request_id = str(uuid4())

        event_body = ObjectBody(
            body={
                "callback_event_type": callback_event_type,
                "chain": normalized_chain,
                "chain_request_id": chain_request_id,
                "job_id": job.job_id,
                "job_type": job.job_type,
            },
            schema=LakeChainRequestEventBodySchema,
        )

        logging.debug(f"sending chain request: {event_body}")

        chain_inception_run = ChainInceptionRun(
            chain_request_id=chain_request_id,
            lake_request_id=lake_request_id,
            job_id=job.job_id,
            job_type=job.job_type,
        )

        if validate_lock_id:
            mutex_client = InceptionMutexClient()

            holds_lock = mutex_client.validate_lock(lake_request_id=lake_request_id, lock_id=validate_lock_id)

            if not holds_lock:
                logging.info(f"lake request {lake_request_id} does not hold lock {validate_lock_id} ... skipping chain request")

                return

        self.chain_inception_run.put(chain_inception_run=chain_inception_run)

        self.event_publisher.submit(
            event=EventBusEvent(
                body=event_body,
                event_type=event_body['event_type'],
            )
        )

        logging.debug(f"chain inception run: {chain_inception_run} saved")