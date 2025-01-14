"""
Handles the Lake Requests and Lake Chain Requests
"""
import logging

from da_vinci.core.immutable_object import (
    ObjectBody,
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)

from da_vinci.event_bus.client import EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.api.runtime.base import ChildAPI, Route

from omnilake.internal_lib.event_definitions import (
    LakeChainRequestEventBodySchema,
    LakeRequestEventBodySchema,
)

from omnilake.tables.jobs.client import Job, JobsClient
from omnilake.tables.lake_chain_requests.client import (
    LakeChainRequest,
    LakeChainRequestsClient,
)
from omnilake.tables.lake_requests.client import (
    LakeRequest as LakeRequestObj,
    LakeRequestStage,
    LakeRequestsClient,
)


class LakeRequestSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='lookup_instructions',
            type=SchemaAttributeType.OBJECT_LIST,
        ),

        SchemaAttribute(
            name='processing_instructions',
            type=SchemaAttributeType.OBJECT,
        ),

        SchemaAttribute(
            name='response_config',
            type=SchemaAttributeType.OBJECT,
            default_value={},
            required=False,
        )
    ]


class LakeChainValidationConditionSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='execute_chain_step',
            type=SchemaAttributeType.STRING,
            required=False,
        ),

        SchemaAttribute(
            name='terminate_chain',
            type=SchemaAttributeType.BOOLEAN,
            required=False,
        ),
    ]


class LakeRequestValidationSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='model_id',
            type=SchemaAttributeType.STRING,
            required=False,
        ),

        SchemaAttribute(
            name='on_failure',
            type=SchemaAttributeType.OBJECT,
            object_schema=LakeChainValidationConditionSchema,
            required=False,
        ),

        SchemaAttribute(
            name='on_success',
            type=SchemaAttributeType.OBJECT,
            object_schema=LakeChainValidationConditionSchema,
            required=False,
        ),

        SchemaAttribute(
            name='prompt',
            type=SchemaAttributeType.STRING,
            required=True,
        ),
    ]


class LakeChainStepSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='conditional',
            type=SchemaAttributeType.BOOLEAN,
            default_value=False,
            required=False,
        ),

        SchemaAttribute(
            name='lake_request',
            type=SchemaAttributeType.STRING,
            object_schema=LakeRequestSchema,
            required=True,
        ),

        SchemaAttribute(
            name='name',
            type=SchemaAttributeType.STRING,
            required=True,
        ),

        SchemaAttribute(
            name='validation',
            type=SchemaAttributeType.OBJECT,
            object_schema=LakeRequestValidationSchema,
            required=False,
        ),
    ]


class LakeChainRequestSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='chain',
            type=SchemaAttributeType.OBJECT_LIST,
        ),
    ]


class DescribeChainRequestSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='chain_request_id',
            type=SchemaAttributeType.STRING,
        )
    ]


class DescribeLakeRequestSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='lake_request_id',
            type=SchemaAttributeType.STRING,
        )
    ]


class LakeRequestAPI(ChildAPI):
    routes = [
        Route(
            path='/describe_chain_request',
            method_name='describe_chain_request',
            request_body_schema=DescribeChainRequestSchema,
        ),

        Route(
            path='/describe_lake_request',
            method_name='describe_lake_request',
            request_body_schema=DescribeLakeRequestSchema,
        ),

        Route(
            path='/submit_lake_request',
            method_name='submit_lake_request',
            request_body_schema=LakeRequestSchema,
        ),

        Route(
            path='/submit_chain_request',
            method_name='submit_chain_request',
            request_body_schema=LakeChainRequestSchema,
        ),
    ]

    def describe_chain_request(self, request: ObjectBody):
        """
        Describes a Chain Request

        Keyword arguments:
        request -- The request body
        """
        chain_requests = LakeChainRequestsClient()

        chain_request_id = request["chain_request_id"]

        chain_request = chain_requests.get(chain_request_id=chain_request_id)

        if not chain_request:
            logging.debug(f'Chain request not found: {chain_request_id}')

            return self.respond(
                body={'message': "chain request not found"},
                status_code=404,
            )

        response_body = chain_request.to_dict(json_compatible=True)

        logging.debug(f'Describing chain request: {response_body}')

        return self.respond(
            body=response_body,
            status_code=200,
        )

    def describe_lake_request(self, request: ObjectBody):
        """
        Describes a Lake Request

        Keyword arguments:
        request -- The request body
        """
        lake_requests = LakeRequestsClient()

        lake_request_id = request["lake_request_id"]

        request_obj = lake_requests.get(lake_request_id=lake_request_id)

        if not request_obj:
            logging.debug(f'Lake request not found: {lake_request_id}')

            return self.respond(
                body={'message': "lake request not found"},
                status_code=404,
            )

        response_body = request_obj.to_dict(json_compatible=True)

        logging.debug(f'Describing request: {response_body}')

        return self.respond(
            body=response_body,
            status_code=200,
        )

    def submit_lake_request(self, request: ObjectBody):
        """
        Handles Lake Request and Request Chain submission

        Keyword arguments:
        request -- The request body
        """
        job = Job(job_type='LAKE_REQUEST')

        jobs = JobsClient()

        jobs.put(job)

        lookup_instructions = request['lookup_instructions']

        processing_instructions = request['processing_instructions']

        response_config = request['response_config']

        lake_requests = LakeRequestsClient()

        # Populate initial lake request to obtain the request_id
        lake_request = LakeRequestObj(
            job_id=job.job_id,
            job_type=job.job_type,
            last_known_stage=LakeRequestStage.VALIDATING,
            lookup_instructions=lookup_instructions,
            processing_instructions=processing_instructions,
            response_config=response_config,
        )

        lake_requests.put(lake_request)

        event_body = ObjectBody(
            body={
                "job_id": job.job_id,
                "job_type": job.job_type,
                "lake_request_id": lake_request.lake_request_id,
                "lookup_instructions": lookup_instructions,
                "processing_instructions": processing_instructions,
                "response_config": response_config,
            },
            schema=LakeRequestEventBodySchema,
        )

        event = EventBusEvent(
            event_type=event_body.get("event_type"),
            body=event_body.to_dict(),
        )

        publisher = EventPublisher()

        publisher.submit(event)

        return self.respond(
            body={
                'job_id': job.job_id,
                'job_type': job.job_type,
                'lake_request_id': lake_request.lake_request_id,
            },
            status_code=201,
        )

    def submit_chain_request(self, request: ObjectBody):
        """
        Handles Lake Request and Request Chain submission

        Keyword arguments:
        request -- The request body
        """
        job = Job(job_type='LAKE_CHAIN_REQUEST')

        jobs = JobsClient()

        jobs.put(job)

        req_dict = request.to_dict()

        chain_request_obj = LakeChainRequest(
            chain=req_dict['chain'],
            job_id=job.job_id,
            job_type=job.job_type,
        )

        chain_requests = LakeChainRequestsClient()

        chain_requests.put(chain_request_obj)

        event_body = ObjectBody(
            body={
                "chain": request['chain'],
                "chain_request_id": chain_request_obj.chain_request_id,
                "job_id": job.job_id,
                "job_type": job.job_type,
            },
            schema=LakeChainRequestEventBodySchema,
        )

        event = EventBusEvent(
            body=event_body.to_dict(),
            event_type=event_body.get("event_type"),
        )

        publisher = EventPublisher()

        publisher.submit(event)

        return self.respond(
            body={
                'chain_request_id': chain_request_obj.chain_request_id,
                'job_id': job.job_id,
                'job_type': job.job_type,
            },
            status_code=201,
        )