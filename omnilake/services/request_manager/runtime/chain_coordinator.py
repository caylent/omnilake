"""
Handles coordinating LakeRequest Chains
"""
import logging

from copy import deepcopy
from datetime import datetime, UTC as utc_tz
from typing import Dict, List, Optional, Union

from da_vinci.core.immutable_object import (
    ObjectBody,
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)
from da_vinci.core.logging import Logger

from da_vinci.event_bus.client import EventPublisher, fn_event_response
from da_vinci.event_bus.event import Event as EventBusEvent

from da_vinci.exception_trap.client import ExceptionReporter

from omnilake.internal_lib.clients import RawStorageManager
from omnilake.internal_lib.event_definitions import (
    LakeChainRequestEventBodySchema,
    LakeCompletionEventBodySchema,
    LakeRequestEventBodySchema,
)

from omnilake.tables.jobs.client import JobsClient, JobStatus
from omnilake.tables.lake_chain_requests.client import (
    LakeChainRequest,
    LakeChainRequestsClient,
    LakeChainRequestStatus,
)
from omnilake.tables.lake_requests.client import (
    LakeRequest,
    LakeRequestsClient,
    LakeRequestStage,
    LakeRequestStatus,
)

# Local imports
from omnilake.services.request_manager.runtime.chain_validation import (
    ChainNode,
    ValidateChain,
)
from omnilake.services.request_manager.runtime.request_init import LakeRequestInit
from omnilake.services.request_manager.runtime.response_validation import validate_response

from omnilake.services.request_manager.tables.lake_chain_coordinated_lake_requests.client import (
    LakeChainCoordinatedLakeRequest,
    LakeChainCoordinatedLakeRequestsClient,
    CoordinatedLakeRequestStatus,
    CoordinatedLakeRequestValidationStatus,
)


class LakeChainRequestValidationConditionSchema(ObjectBodySchema):
    """
    Represents a validation condition for a LakeRequest Chain
    """
    attributes = [
        SchemaAttribute(
            name="lake_request_name",
            type=SchemaAttributeType.STRING,
            required=False
        ),

        SchemaAttribute(
            name="terminate_chain",
            type=SchemaAttributeType.BOOLEAN,
            required=False
        ),
    ]


class LakeChainRequestValidationSchema(ObjectBodySchema):
    """
    Represents a validation schema for a LakeRequest Chain
    """
    attributes = [
        SchemaAttribute(
            name="model_id",
            type=SchemaAttributeType.STRING,
            required=False
        ),
        SchemaAttribute(
            name="on_failure",
            type=SchemaAttributeType.OBJECT,
            required=False,
            object_schema=LakeChainRequestValidationConditionSchema
        ),
        SchemaAttribute(
            name="on_success",
            type=SchemaAttributeType.OBJECT,
            required=False,
            object_schema=LakeChainRequestValidationConditionSchema,
        ),
        SchemaAttribute(
            name="prompt",
            type=SchemaAttributeType.STRING,
            required=False
        ),
    ]


class LakeChainRequestSchema(ObjectBodySchema):
    """
    Represents a request in a LakeRequest Chain
    """
    attributes = [
        SchemaAttribute(
            name="conditional",
            type=SchemaAttributeType.BOOLEAN
        ),
        SchemaAttribute(
            name="lake_request",
            type=SchemaAttributeType.OBJECT
        ),
        SchemaAttribute(
            name="name",
            type=SchemaAttributeType.STRING
        ),
        SchemaAttribute(
            name="validation",
            type=SchemaAttributeType.OBJECT,
            required=False,
            object_schema=LakeChainRequestValidationSchema,
        ),
    ]


class LakeChainSchema(ObjectBodySchema):
    """
    Represents a LakeRequest Chain
    """
    attributes = [
        SchemaAttribute(name="requests", object_schema=LakeChainRequestSchema, type=SchemaAttributeType.OBJECT_LIST),
    ]


class LakeChainExecutedRequestSchema(ObjectBodySchema):
    """
    Represents a request that has been executed in a LakeRequest Chain
    """
    attributes = [
        SchemaAttribute(name="name", type=SchemaAttributeType.STRING),
        SchemaAttribute(name="lake_request_id", type=SchemaAttributeType.STRING),
    ]


class ChainReference:
    REFERENCE_PREFIX = "REF:"

    SUPPORTED_REFERENCE_TYPES = [
        "response_id", # Replaces the reference with the Lake Request ID of the referenced response
        "response_body", # Replaces the reference with the content of the referenced response
    ]

    def __init__(self, reference_str: str):
        """
        Initializes a new ChainReference object.

        Keyword Arguments:
        reference -- The reference to load
        """
        if not self.is_reference(reference_str):
            raise ValueError(f"Invalid reference: {reference_str}")

        self.full_reference_str = reference_str

        self.reference_str = reference_str[len(self.REFERENCE_PREFIX):]

        # Splits a reference <lake_request_id>.<lake_request_type> (e.g. previous_request.response_id)
        try:
            self.reference_request_name, self.reference_request_type = self.reference_str.split('.')
        except ValueError:
            raise ValueError(f"Invalid reference: '{reference_str}' expected format: <request_name>.<request_type>")

        if self.reference_request_type not in self.SUPPORTED_REFERENCE_TYPES:
            raise ValueError(f"Unsupported reference type: {self.reference_request_type}")

    def _load_response_body(self, request_id: str) -> str:
        """
        Loads the response body of the given request
        """
        lake_requests_client = LakeRequestsClient()

        lake_request = lake_requests_client.get(lake_request_id=request_id)

        logging.debug(f"Loaded lake request: {lake_request}")

        response_entry_id = lake_request.response_entry_id

        storage_manager_client = RawStorageManager()

        entry_resp = storage_manager_client.get_entry(entry_id=response_entry_id)

        content = entry_resp.response_body["content"]

        logging.debug(f"Loaded response content for {response_entry_id}: {content}")

        return content

    def dereference(self, request_name_to_id_map: Dict[str, str]) -> str:
        """
        Returns the dereferenced value of the reference

        Keyword Arguments:
        request_name_to_id_map -- A map of request names to their IDs
        """
        if self.reference_request_name not in request_name_to_id_map:
            raise ValueError(f"Could not find request ID for request name: {self.reference_request_name}")

        request_id = request_name_to_id_map[self.reference_request_name]

        if self.reference_request_type == "response_id":
            return request_id

        return self._load_response_body(request_id=request_id)

    @classmethod
    def is_reference(cls, reference_str: str) -> bool:
        """
        Returns True if the given reference is a reference to a previously executed request
        """
        return reference_str.startswith(cls.REFERENCE_PREFIX)


class ChainRequest:
    def __init__(self, request: ObjectBody):
        """
        Initializes a new ChainRequest object.

        Keyword Arguments:
        request -- The request to load
        """
        logging.debug(f"Loading request: {request}")

        self.request = request.map_to(new_schema=LakeChainRequestSchema)

        logging.debug(f"Loaded request: {self.request.to_dict()}")

        self.conditional = self.request["conditional"]

        validation = self.request.get("validation")

        self.validation_instructions = validation.get("prompt") if validation else None

        self.validation_model_id = validation.get("model_id") if validation else None

        self.on_failure = {}

        self.on_success = {}

        if validation:
            validation_body = ObjectBody(body=validation, schema=LakeChainRequestValidationSchema)

            self.on_failure = validation_body.get("on_failure") or {}

            self.on_success = validation_body.get("on_success") or {}

        self.raw_lake_request = self.request["lake_request"]

        self.name = self.request["name"]

        # Will be used to store any names of requests that this request depends on directly for references
        self.direct_references = self.referenced_chain_request_names(lake_request=self.raw_lake_request)

        self.dep_tree_node = ChainNode(
            name=self.name,
            conditional=self.conditional,
            direct_references=self.direct_references,
            on_failure_reference=self.on_failure.get("lake_request_name"),
            on_success_reference=self.on_success.get("lake_request_name"),
        )

    @staticmethod
    def referenced_attribute_names(instruction_obj: Union[ObjectBody, Dict]) -> List[str]:
        """
        Returns the names of all attributes that are references in the given instruction object

        Keyword Arguments:
        instruction_obj -- The instruction object to search for references
        """
        references = []

        instruction_obj_dict = instruction_obj

        if isinstance(instruction_obj, ObjectBody):
            instruction_obj_dict = instruction_obj.to_dict()

        logging.debug(f"Processing instruction object for references: {instruction_obj_dict}")

        for attr in instruction_obj_dict:
            logging.debug(f"Checking attribute {attr} for references")

            attr_val = instruction_obj_dict[attr] 

            logging.debug(f"Attribute value: {attr_val}")

            if not isinstance(attr_val, str):
                logging.debug(f"Attribute value is not a string ... skipping")
                continue

            if ChainReference.is_reference(attr_val):
                logging.debug(f"Detected reference: {attr_val}")

                references.append(attr)

            else:
                logging.debug(f"Attribute value is not a reference ... skipping")

        return references

    @classmethod
    def referenced_chain_request_names(cls, lake_request: Union[ObjectBody, Dict]) -> List[str]:
        """
        Returns a list of all references in the given lake request

        Keyword Arguments:
        lake_request -- The lake request to search for references
        """
        lake_request_dict = lake_request
        
        if isinstance(lake_request, ObjectBody):
            lake_request_dict = lake_request.to_dict()

        logging.debug(f"Processing lake request for references: {lake_request_dict}")

        referenced_names = set()

        # Handle Lookup Instructions
        for lookup_instruction in lake_request_dict["lookup_instructions"]:

            req_type = lookup_instruction["request_type"]

            logging.debug(f"Checking lookup instruction of type {req_type} for references")

            ref_attr_names = cls.referenced_attribute_names(lookup_instruction)

            if ref_attr_names:
                for ref_attr_name in ref_attr_names:
                    reference = ChainReference(lookup_instruction[ref_attr_name])

                    referenced_names.add(reference.reference_request_name)

        for instruction_name in ["processing_instructions", "response_config"]:
            logging.debug(f"Checking instruction set {instruction_name} in {lake_request_dict} for references")

            instruction = lake_request_dict[instruction_name]

            logging.debug(f"Processing instruction set for references: {instruction}")

            ref_attr_names = cls.referenced_attribute_names(instruction)

            logging.debug(f"Referenced attribute names: {ref_attr_names}")

            if ref_attr_names:
                for ref_attr_name in ref_attr_names:
                    reference = ChainReference(instruction[ref_attr_name])

                    referenced_names.add(reference.reference_request_name)

        logging.debug(f"Referenced names: {referenced_names}")

        return referenced_names

    def can_execute(self, completed_executed_request_names: List[str]) -> bool:
        """
        Returns True if the request can be executed

        Keyword Arguments:
        completed_executed_request_names -- The names of all requests that have been executed
        """
        if not self.direct_references:
            return True
        
        return all([ref in completed_executed_request_names for ref in self.direct_references])

    def dereferenced_request(self, request_name_to_id_map: Dict[str, str]) -> ObjectBody:
        """
        Returns the LakeRequest object with all references dereferenced

        Keyword Arguments:
        request_name_to_id_map -- A map of request names to their IDs
        """
        new_body = {
            "lookup_instructions": [],
            "processing_instructions": {}, 
            "response_config": {},
        }

        # Convert to Dict to enable mutation
        raw_lake_request = self.raw_lake_request.to_dict()

        logging.debug(f"Dereferencing request '{raw_lake_request}' using map: {request_name_to_id_map}")

        lookup_instruction = raw_lake_request["lookup_instructions"]

        for instruction in lookup_instruction:
            lookup_instruction = {}

            referenced_attr_names = self.referenced_attribute_names(instruction_obj=instruction)

            if not referenced_attr_names:
                new_body["lookup_instructions"].append(instruction)

                continue

            new_instruction_body = deepcopy(instruction)

            for attr_name in referenced_attr_names:
                reference = ChainReference(instruction[attr_name])

                new_instruction_body[attr_name] = reference.dereference(request_name_to_id_map=request_name_to_id_map)

            new_body["lookup_instructions"].append(new_instruction_body)

        for instruction_set in ["processing_instructions", "response_config"]:
            new_instruction_body = deepcopy(raw_lake_request[instruction_set])

            for attr_name in self.referenced_attribute_names(instruction_obj=new_instruction_body):
                reference = ChainReference(new_instruction_body[attr_name])

                new_instruction_body[attr_name] = reference.dereference(request_name_to_id_map=request_name_to_id_map)

            new_body[instruction_set] = new_instruction_body

        logging.debug(f"Dereferenced request: {new_body}")

        return ObjectBody(body=new_body)


def calculate_unexecuted_request_names(chain_request_id: str, chain: List[Union[Dict[str, str], ObjectBody]]):
    """
    Calculates the requests that have not been executed

    Keyword Arguments:
    chain_request_id -- The ID of the chain request
    chain -- The chain of requests to check
    """
    request_names = set([req["name"] for req in chain])

    coordinated_requests = LakeChainCoordinatedLakeRequestsClient()

    all_coordinated = coordinated_requests.get_all_by_chain_request_id(chain_request_id=chain_request_id)

    all_coordinated_names = set([req.chain_request_name for req in all_coordinated])

    return request_names - all_coordinated_names


class ChainCoordinator:
    """
    Handles coordinating LakeRequest Chains
    """
    def __init__(self, chain: ObjectBody, chain_request_id: Optional[str] = None,
                 conditions_met_requests: Optional[List[str]] = None, executed_requests: Optional[Dict[str, str]] = None):
        """
        Initializes a new ChainCoordinator object.

        Keyword Arguments:
        chain -- The full chain of requests to be executed
        chain_request_id -- The ID of the request chain
        conditions_met_requests -- The list of requests that have had their conditions met
        executed_requests -- The mappping of previously executed requests
        """
        logging.debug(f"Loading chain: {chain}")

        self.chain = chain.map_to(new_schema=LakeChainSchema)

        requests = self.chain["requests"]

        self.requests = [ChainRequest(request=req) for req in requests]

        self.conditions_met_requests = conditions_met_requests or []

        self.executed_requests = {}

        self.chain_request_id = chain_request_id

        self.executed_requests = executed_requests or {}

        self.event_publisher = EventPublisher()

    @classmethod
    def from_chain_request_id(cls, chain_request_id: str) -> 'ChainCoordinator':
        """
        Loads a ChainCoordinator object from a chain request ID

        Keyword Arguments:
        chain_request_id -- The ID of the chain request to load
        """
        logging.debug(f"Loading chain coordinator for chain request ID: {chain_request_id}")

        chain_requests = LakeChainRequestsClient()

        chain_request = chain_requests.get(chain_request_id=chain_request_id, consistent_read=True)

        if not chain_request:
            raise ValueError(f"Could not find chain request with ID: {chain_request_id}")

        logging.debug(f"Loaded chain request: {chain_request.to_dict()}")

        return cls(
            chain=ObjectBody(body={"requests": chain_request.chain}),
            chain_request_id=chain_request_id,
            conditions_met_requests=chain_request.conditions_met_requests,
            executed_requests=chain_request.executed_requests,
        )

    def has_executed(self, request_name: str) -> bool:
        """
        Returns True if the given request has already been executed

        Keyword Arguments:
        request_name -- The name of the request to check
        """
        return request_name in self.executed_requests

    def _next_available_request_group(self) -> Dict[str, Dict[str, Union[str, ObjectBody]]]: 
        """
        Returns the next group of lake requests that can be executed
        """
        logging.debug("Determining next group of requests to execute ...")
        next_group = {}

        unexecuted_names = calculate_unexecuted_request_names(
            chain=self.chain["requests"],
            chain_request_id=self.chain_request_id,
        )

        completed_executed_request_names = list(self.executed_requests.keys())

        for request in self.requests:
            # Continue for already executed requests
            if request.name not in unexecuted_names:
                logging.debug(f"Request {request.name} has already been executed ... skipping")

                continue

            if request.can_execute(completed_executed_request_names=completed_executed_request_names):
                if request.conditional:
                    logging.debug(f"Request {request.name} is conditional ... checking if it can be executed")

                    if request.name in self.conditions_met_requests:
                        next_group[request.name] = {
                            "request": request.dereferenced_request(request_name_to_id_map=self.executed_requests),
                            "validation_instructions": request.validation_instructions,
                            "validation_model_id": request.validation_model_id,
                        }

                        continue

                else:
                    logging.debug(f"Request {request.name} is not conditional and dependencies met ... executing")

                    next_group[request.name] = {
                        "request": request.dereferenced_request(request_name_to_id_map=self.executed_requests),
                        "validation_instructions": request.validation_instructions,
                        "validation_model_id": request.validation_model_id,
                    }

        return next_group

    def execute_next(self, parent_job_id: str, parent_job_type: str) -> int:
        """
        Executes the next group of requests in the chain

        Keyword Arguments:
        parent_job_id -- The ID of the parent job
        parent_job_type -- The type of the parent job

        Returns the number of requests that were executed
        """
        next_group = self._next_available_request_group()

        if not next_group:
            logging.debug("No more requests to execute ... exiting")

            return 0

        jobs = JobsClient()

        parent_job = jobs.get(job_id=parent_job_id, job_type=parent_job_type)

        lake_requests = LakeRequestsClient()

        coordinated_requests = LakeChainCoordinatedLakeRequestsClient()

        for req_name, request_details in next_group.items():
            logging.debug(f"Executing request '{req_name}': {request_details}")

            request = request_details["request"]

            child_job = parent_job.create_child(job_type="LAKE_REQUEST")

            jobs.put(child_job)

            lake_request = LakeRequest(
                job_id=child_job.job_id,
                job_type=child_job.job_type,
                last_known_stage=LakeRequestStage.VALIDATING,
                lookup_instructions=request["lookup_instructions"],
                processing_instructions=request["processing_instructions"],
                response_config=request["response_config"],
            )

            lake_requests.put(lake_request)

            req_obj = ObjectBody(
                body={
                    "job_id": child_job.job_id,
                    "job_type": child_job.job_type,
                    "lake_request_id": lake_request.lake_request_id,
                    "lookup_instructions": request["lookup_instructions"],
                    "processing_instructions": request["processing_instructions"],
                    "response_config": request["response_config"],
                },
                schema=LakeRequestEventBodySchema,
            )

            self.event_publisher.submit(
                event=EventBusEvent(
                    body=req_obj.to_dict(),
                    event_type=req_obj.get("event_type"),
                )
            )

            # Workaround to ensure the chain request id is set since it isn't required to initialize the object
            if not self.chain_request_id:
                raise ValueError("Chain request ID not set")

            coordinated_request = LakeChainCoordinatedLakeRequest(
                chain_request_id=self.chain_request_id,
                chain_request_name=req_name,
                execution_status=CoordinatedLakeRequestStatus.RUNNING,
                lake_request_id=lake_request.lake_request_id,
                validation_instructions=request_details["validation_instructions"],
                validation_model_id=request_details["validation_model_id"],
            )

            coordinated_requests.put(coordinated_request)

        return len(next_group)

    def request_by_name(self, name: str):
        """
        Returns the request with the given name

        Keyword Arguments:
        name -- The name of the request to retrieve
        """
        for request in self.requests:
            if request.name == name:
                return request

        return None

    def to_dict(self):
        """
        Returns the ChainCoordinator object as a dictionary
        """
        return {
            "chain": self.chain.to_dict(),
            "executed_requests": self.executed_requests,
        }

    def validate_chain(self):
        """
        Validates the chain
        """
        # Validate the chain dependency and execution structure
        ValidateChain()(chain_nodes=[req.dep_tree_node for req in self.requests])

        # Validate each LakeRequest in the chain
        lk_req_init = LakeRequestInit()

        for request in self.requests:
            lk_req_init.validate(body=request.raw_lake_request)


def __close_chain(chain: LakeChainRequest, chain_status: LakeChainRequestStatus, job_status: JobStatus):
    """
    Closes out a chain request

    Keyword Arguments:
    chain -- The chain request to close
    chain_status -- The status to set the chain to
    job_status -- The status to set the parent job to
    """
    jobs = JobsClient()

    parent_job = jobs.get(job_id=chain.job_id, job_type=chain.job_type)

    parent_job.status = job_status

    end_time = datetime.now(tz=utc_tz)

    parent_job.ended = end_time

    jobs.put(parent_job)

    chain.ended = end_time

    chain.chain_execution_status = chain_status

    chain.unexecuted_request_names = calculate_unexecuted_request_names(
        chain=chain.chain,
        chain_request_id=chain.chain_request_id,
    )

    chains = LakeChainRequestsClient()

    chains.put(chain)


_FN_NAME = 'omnilake.services.request_manager.chain_coordinator.handle_lake_response'


@fn_event_response(function_name=_FN_NAME, exception_reporter=ExceptionReporter(), logger=Logger(_FN_NAME))
def handle_lake_response(event, context):
    """
    Handles all lake request completion events
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=LakeCompletionEventBodySchema,
    )

    lake_request_id = event_body["lake_request_id"]

    coordinated_requests = LakeChainCoordinatedLakeRequestsClient()

    running_request = coordinated_requests.get_by_lake_request_id(lake_request_id=lake_request_id)

    if not running_request:
        logging.debug(f"Could not find running request for lake request id {lake_request_id}, nothing to coordinate")

        return

    lake_requests = LakeRequestsClient()

    lake_request = lake_requests.get(lake_request_id=lake_request_id)

    running_request.execution_status = CoordinatedLakeRequestStatus.COMPLETED

    if lake_request.request_status == LakeRequestStatus.FAILED:
        running_request.execution_status = CoordinatedLakeRequestStatus.FAILED

    coordinated_requests.put(running_request=running_request)

    chains = LakeChainRequestsClient()

    # Check if the Lake Request failed
    if lake_request.request_status != LakeRequestStatus.COMPLETED:
        logging.debug("Lake request did not complete successfully ... failing chain")

        chain = chains.get(lake_chain_request_id=running_request.chain_request_id)

        __close_chain(chain=chain, chain_status=LakeChainRequestStatus.FAILED, job_status=JobStatus.FAILED)

        return

    # Could be re-evaluated for redundancy. Using updates to the chain request since this
    # is potentially being called by multiple instances of the function
    chains.record_lake_request_results(
        chain_request_id=running_request.chain_request_id,
        lake_request_id=lake_request_id,
        reference_name=running_request.chain_request_name,
    )

    coordinator = ChainCoordinator.from_chain_request_id(chain_request_id=running_request.chain_request_id)

    # Check if the request is a validation request
    if running_request.validation_instructions:
        logging.debug("Request is a validation request ... executing validation")

        req_info = coordinator.request_by_name(name=running_request.chain_request_name)

        chain_details = chains.get(chain_request_id=running_request.chain_request_id)

        validation_status = validate_response(
            parent_job_id=chain_details.job_id,
            parent_job_type=chain_details.job_type,
            lake_request_id=chain_details.executed_requests[req_info.name],
            validation_instructions=running_request.validation_instructions,
            validation_model_id=running_request.validation_model_id,
        )

        running_request.validation_status = validation_status

        coordinated_requests.put(running_request)

        if validation_status:
            _v_status_to_body_map = {
                CoordinatedLakeRequestValidationStatus.FAILURE: req_info.on_failure,
                CoordinatedLakeRequestValidationStatus.SUCCESS: req_info.on_success,
            }

            validation_body = _v_status_to_body_map[validation_status]

            if validation_body.get("terminate_chain", False):
                logging.debug("Terminating chain ...")

                chain = chains.get(chain_request_id=running_request.chain_request_id)

                # A failure terminate will always fail the chain
                if validation_status == CoordinatedLakeRequestValidationStatus.FAILURE:
                    __close_chain(chain=chain, chain_status=LakeChainRequestStatus.FAILED, job_status=JobStatus.FAILED)

                    return

                __close_chain(chain=chain, chain_status=LakeChainRequestStatus.COMPLETED, job_status=JobStatus.COMPLETED)

                return

            elif validation_body.get("execute_chain_step"):
                logging.debug("Unlocking conditional request ...")

                chains.add_condition_met_request(
                    chain_request_id=running_request.chain_request_id,
                    request_name=validation_body["execute_chain_step"]
                )

            else:
                logging.debug("Ran a validation request just to run it :shrug: ... nothing to do")

    # Determine if there are any more requests to execute
    chain_details = chains.get(chain_request_id=running_request.chain_request_id, consistent_read=True)

    if chain_details.chain_execution_status in [LakeRequestStatus.COMPLETED, LakeRequestStatus.FAILED]:
        logging.debug("Chain has already been completed ... nothing to do")

        return

    coordinator = ChainCoordinator(
        chain=ObjectBody(body={"requests": chain_details.chain}),
        chain_request_id=running_request.chain_request_id,
        conditions_met_requests=chain_details.conditions_met_requests,
        executed_requests=chain_details.executed_requests,
    )

    # Execute the next group of requests in the chain
    logging.debug("Executing next group of requests in chain")

    num_of_executed = coordinator.execute_next(
        parent_job_id=chain_details.job_id,
        parent_job_type=chain_details.job_type,
    )

    if num_of_executed == 0:
        logging.debug("No requests available to execute right now ... nothing to do")

        all_coordinated_by_chain = coordinated_requests.get_all_by_chain_request_id(chain_request_id=running_request.chain_request_id)

        logging.debug(f"Found {all_coordinated_by_chain} coordinated requests")

        # Check if all requests have been executed
        if all([req.execution_status == 'COMPLETED' or req.execution_status == 'FAILED' for req in all_coordinated_by_chain]):
            # Close out the chain
            logging.debug("All requests have finished ... closing out chain")

            __close_chain(chain=chain_details, chain_status=LakeChainRequestStatus.COMPLETED, job_status=JobStatus.COMPLETED)

            return

        else:
            # Still pending other requests for the chain to complete
            logging.debug("Still waiting on other lake requests to complete ... nothing to do")

            return
    else:
        logging.debug(f"Executed {num_of_executed} new requests ... exiting")

        chains.increment_remaining_running_requests(
            chain_request_id=running_request.chain_request_id,
            increment_by=num_of_executed
        )


_FN_NAME = 'omnilake.services.request_manager.chain_coordinator.initiate_chain'


@fn_event_response(function_name=_FN_NAME, exception_reporter=ExceptionReporter(), logger=Logger(_FN_NAME))
def handle_initiate_chain(event, context):
    """
    Handles incoming new chain requests
    """
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=LakeChainRequestEventBodySchema,
    )

    logging.debug("Loading job")

    jobs = JobsClient()

    job = jobs.get(job_id=event_body["job_id"], job_type=event_body["job_type"])

    logging.debug("Job loaded")

    with jobs.job_execution(job=job, skip_completion=True):
        logging.debug("Initializing new ChainCoordinator object")

        coordinator = ChainCoordinator.from_chain_request_id(chain_request_id=event_body["chain_request_id"])

        logging.debug("Validating chain")

        coordinator.validate_chain()

        logging.debug("Chain validation complete")

        chains = LakeChainRequestsClient()

        # Set the chain status to executing
        chain = chains.get(chain_request_id=event_body["chain_request_id"])

        chain.chain_execution_status = LakeChainRequestStatus.EXECUTING

        chains.put(chain)

        # Execute the next group of requests in the chain
        logging.debug("Executing next group of requests in chain")

        num_executed = coordinator.execute_next(
            parent_job_id=event_body["job_id"],
            parent_job_type=event_body["job_type"],
        )

        chains.increment_remaining_running_requests(
            chain_request_id=event_body["chain_request_id"],
            increment_by=num_executed,
        )