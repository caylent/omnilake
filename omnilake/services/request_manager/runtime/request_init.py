"""
Handle Initial Lake Request Processing
"""
import logging

from dataclasses import dataclass
from typing import Dict, List, Optional, Union

from da_vinci.core.immutable_object import (
    InvalidObjectSchemaError,
    ObjectBody,
    ObjectBodySchema,
)

from da_vinci.core.logging import Logger

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from da_vinci.exception_trap.client  import ExceptionReporter

from omnilake.internal_lib.event_definitions import (
    LakeRequestEventBodySchema,
    LakeRequestInternalRequestEventBodySchema,
)

from omnilake.tables.jobs.client import JobsClient
from omnilake.tables.lake_requests.client import (
    LakeRequestsClient,
    LakeRequestStage,
    LakeRequestStatus,
)
from omnilake.tables.registered_request_constructs.client import (
    RequestConstructType,
    RegisteredRequestConstructsClient,
)

# Local Imports
from omnilake.services.request_manager.runtime.stage_complete import CALLBACK_ON_FAILURE_EVENT_TYPE

from omnilake.services.request_manager.runtime.primitive_lookup import (
    BulkEntryLookupSchema,
    DirectEntryLookupSchema,
    DirectSourceLookupSchema,
    RelatedRequestResponseLookupSchema,
    RelatedRequestSourcesLookupSchema,
)


class RequestValidationError(ValueError):
    def __init__(self, message: str):
        super().__init__(message)


@dataclass
class LoadedConstruct:
    event_type: str
    schema: ObjectBodySchema


class LakeRequestInit:
    def __init__(self, originating_event: Optional[EventBusEvent] = None):
        """
        Initialize the Lake Request Init service

        Keyword Arguments:
        originating_event -- The event that triggered the request, this is optional to be able to use this obejct in 
                                a standalone manner for request validation for chains
        """
        self.registered_constructs = RegisteredRequestConstructsClient()

        self.event_publisher = EventPublisher()

        self.originating_event = originating_event

        self.__lookup_primitive_schemas = {
            "BULK_ENTRY": BulkEntryLookupSchema,
            "DIRECT_ENTRY": DirectEntryLookupSchema,
            "DIRECT_SOURCE": DirectSourceLookupSchema,
            "RELATED_RESPONSE": RelatedRequestResponseLookupSchema,
            "RELATED_SOURCES": RelatedRequestSourcesLookupSchema,
        }

        self._loaded_constructs = {}

    def _get_construct(self, operation_name: str, registered_construct_type: str, registered_construct_name: str) -> LoadedConstruct:
        """
        Get a registered construct

        Keyword Arguments:
        operation_name -- The operation to perform on the construct
        registered_construct_type -- The type of the registered construct
        registered_construct_name -- The name of the registered construct
        """
        loaded_construct_name = f"{registered_construct_type}_{registered_construct_name}"

        # Perform lookup if construct is not loaded
        if loaded_construct_name not in self._loaded_constructs:

            if registered_construct_type == RequestConstructType.ARCHIVE and \
                  registered_construct_name in self.__lookup_primitive_schemas:

                self._loaded_constructs[loaded_construct_name] = LoadedConstruct(
                    event_type="omnilake_lake_request_primitive_lookup",
                    schema=self.__lookup_primitive_schemas[registered_construct_name],
                )

                return self._loaded_constructs[loaded_construct_name]

            registered_construct = self.registered_constructs.get(
                registered_type_name=registered_construct_name,
                registered_construct_type=registered_construct_type,
            )

            logging.debug(f"Loaded construct {registered_construct_name} for {operation_name}")

            if registered_construct is None:
                raise RequestValidationError(f"Registered construct {registered_construct_name} not found")

            self._loaded_constructs[loaded_construct_name] = LoadedConstruct(
                event_type=registered_construct.get_operation_event_name(operation=operation_name),
                schema=registered_construct.get_object_body_schema(operation=operation_name),
            )

        return self._loaded_constructs[loaded_construct_name]

    def _load_construct_schema(self, registered_construct_type: str, registered_construct_name: str, schema_operation: str) -> str:
        """
        Load a registered construct from the database

        Keyword Arguments:
        registered_construct_type -- The type of the registered construct
        registered_construct_name -- The name of the registered construct
        schema_operation -- The operation to perform on the schema
        """
        construct = self._get_construct(
            operation_name=schema_operation,
            registered_construct_type=registered_construct_type,
            registered_construct_name=registered_construct_name,
        )

        logging.debug(f"Loaded construct {registered_construct_name} for {schema_operation}: {construct.schema}")

        return construct.schema

    def _validate_component_schema(self, registered_construct_type: str, registered_construct_name: str,
                                   component_body: ObjectBody) -> None:
        """
        Validate the schema of a component of the request

        Keyword Arguments:
        registered_construct_type -- The type of the registered construct
        registered_construct_name -- The name of the registered construct
        component_body -- The body of the component to validate
        """
        if registered_construct_type == RequestConstructType.ARCHIVE:
            schema = self._load_construct_schema(
                registered_construct_name=registered_construct_name,
                registered_construct_type=registered_construct_type,
                schema_operation="lookup",
            )

        else:
            if registered_construct_type == RequestConstructType.PROCESSOR:
                schema_operation = "process"

            elif registered_construct_type == RequestConstructType.RESPONDER:
                schema_operation = "respond"

            else:
                raise RequestValidationError(f"Invalid construct type {registered_construct_type}")

            schema = self._load_construct_schema(
                registered_construct_name=registered_construct_name,
                registered_construct_type=registered_construct_type,
                schema_operation=schema_operation,
            )

        if schema is None:
            raise RequestValidationError(f"Schema not found for {registered_construct_name}")

        # Cast to schema object, will raise error if not valid
        try:
            component_body.map_to(new_schema=schema)

        except InvalidObjectSchemaError as excp:
            raise RequestValidationError(f"Invalid schema for {registered_construct_name} of type {registered_construct_type}") from excp

    def _publish_lookup_request(self, lookup_instruction: ObjectBody, parent_job_id: str, parent_job_type: str):
        """
        Publish a single lookup request

        Keyword Arguments:
        lookup_instruction -- The lookup instruction to publish
        parent_job_id -- The ID of the parent job
        parent_job_type -- The type of the parent job
        """
        logging.debug(f"Publishing lookup request: {lookup_instruction}")

        construct_details = self._get_construct(
            operation_name="lookup",
            registered_construct_type=RequestConstructType.ARCHIVE,
            registered_construct_name=lookup_instruction["request_type"],
        )

        instruction_body = lookup_instruction.map_to(new_schema=construct_details.schema)

        publish_body = ObjectBody(
            body={
                "lake_request_id": self.originating_event.body["lake_request_id"],
                "parent_job_id": parent_job_id,
                "parent_job_type": parent_job_type,
                "request_body": instruction_body,
            },
            schema=LakeRequestInternalRequestEventBodySchema,
        )

        self.event_publisher.submit(
            event=self.originating_event.next_event(
                event_type=construct_details.event_type,
                body=publish_body.to_dict(),
                callback_event_type_on_failure=CALLBACK_ON_FAILURE_EVENT_TYPE,
            )
        )

        logging.debug(f"Published lookup request to '{construct_details.event_type}': {lookup_instruction}")

    def publish_lookup_requests(self, instructions: List[ObjectBody], parent_job_id: str, parent_job_type: str):
        """
        Publishes requets to 

        Keyword Arguments:
        instructions -- The instructions to publish
        parent_job_id -- The ID of the parent job
        parent_job_type -- The type of the parent job
        """
        for instruction in instructions:
            self._publish_lookup_request(
                lookup_instruction=instruction,
                parent_job_id=parent_job_id,
                parent_job_type=parent_job_type,
            )

    def validate(self, body: Union[Dict, ObjectBody]):
        """
        Validate the body of the request

        Keyword Arguments:
        body -- The body of the request
        """
        if not isinstance(body, ObjectBody):
            body = ObjectBody(
                body=body,
                schema=LakeRequestEventBodySchema,
            )

        # Validate lookup instructions
        lookup_instructions = body["lookup_instructions"]

        for instruction in lookup_instructions:
            self._validate_component_schema(
                registered_construct_type=RequestConstructType.ARCHIVE,
                registered_construct_name=instruction["request_type"],
                component_body=instruction,
            )

        # Validate processing instructions
        processing_instructions = body.get("processing_instructions")

        self._validate_component_schema(
            registered_construct_type=RequestConstructType.PROCESSOR,
            registered_construct_name=processing_instructions["processor_type"],
            component_body=processing_instructions,
        )

        # Validate response config
        response_config = body["response_config"]

        self._validate_component_schema(
            registered_construct_type=RequestConstructType.RESPONDER,
            registered_construct_name=response_config["response_type"],
            component_body=response_config
        )


_FN_NAME = 'omnilake.services.request_manager.lake_request_init'


@fn_event_response(function_name=_FN_NAME, exception_reporter=ExceptionReporter(), logger=Logger(_FN_NAME))
def handler(event, context):
    """
    Handler for Lake Request initialization
    """
    logging.debug(f'Received request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=LakeRequestEventBodySchema,
    )

    jobs = JobsClient()

    request_job = jobs.get(job_id=event_body.get("job_id"), job_type=event_body.get("job_type"))

    with jobs.job_execution(request_job, skip_completion=True):
        validation_job = request_job.create_child("LAKE_REQUEST_VALIDATION")

        jobs.put(job=request_job)

        lake_request_init = LakeRequestInit(originating_event=source_event)

        lake_requests = LakeRequestsClient()

        lake_request = lake_requests.get(lake_request_id=event_body.get("lake_request_id"))

        lake_request.request_status = LakeRequestStatus.PROCESSING

        lake_requests.put(lake_request)

        # Validate the request
        with jobs.job_execution(validation_job, fail_parent=True):
            # TODO: If this fails, kick off a failure event in case a chain is dependent on this
            lake_request_init.validate(event_body)

        lake_request.last_known_stage = LakeRequestStage.LOOKUP

        lookup_instructions = event_body.get("lookup_instructions")

        lake_request.remaining_lookups = len(lookup_instructions)

        lake_requests.put(lake_request)

        # Publish the lookups
        with jobs.job_execution(validation_job, fail_parent=True):
            lake_request_init.publish_lookup_requests(
                instructions=lookup_instructions,
                parent_job_id=request_job.job_id,
                parent_job_type=request_job.job_type,
            )