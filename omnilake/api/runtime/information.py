'''
Handles information requests
'''
import logging

from typing import Dict, List, Optional

from da_vinci.event_bus.client import EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.api.runtime.construct import ChildAPI, Route

from omnilake.internal_lib.event_definitions import InformationRequestBody
from omnilake.internal_lib.job_types import JobType

from omnilake.tables.jobs.client import Job, JobsClient
from omnilake.tables.information_requests.client import (
    InformationRequestsClient,
    InformationRequest as InformationRequestObj
)


def _validate_request(request: Dict):
    """
    Validate the request

    Keyword arguments:
    request -- The request
    """
    # Catch all the globally required keys
    required_keys = ['request_type']

    for key in required_keys:
        if key not in request:
            raise ValueError(f'Missing required key: {key}')


def _validate_requests(requests: List[Dict]):
    """
    Validate the requests

    Keyword arguments:
    requests -- The requests
    """
    for request in requests:
        _validate_request(request)


class InformationRequestAPI(ChildAPI):
    routes = [
        Route(
            path='/describe_request',
            method_name='describe_request',
        ),
        Route(
            path='/request_information',
            method_name='request_information',
        ),
        Route(
            path='/score_response',
            method_name='score_response',
        )
    ]

    def request_information(self, goal: str, retrieval_requests: List[Dict], destination_archive_id: Optional[str] = None,
                            include_source_metadata: Optional[bool] = False, resource_names: Optional[List[str]] = None,
                            responder_use_source_metadata: Optional[bool] = False, responder_model_id: Optional[str] = None,
                            responder_prompt: Optional[str] = None, responder_model_params: Optional[Dict] = None,
                            summarization_algorithm: Optional[str] = 'STANDARD', summarization_prompt: Optional[str] = None,
                            summarization_model_id: Optional[str] = None, summarization_model_params: Optional[Dict] = None):
        """
        Request the system to provide information

        Keyword arguments:
        goal -- The goal
        retrieval_requests -- The requests for retrieving the information
        destination_archive_id -- The destination archive ID
        include_source_metadata -- Whether to include source metadata
        resource_names -- The resource names
        responder_use_source_metadata -- Whether to use source metadata
        responder_model_id -- The responder model ID
        responder_prompt -- The responder prompt
        responder_model_params -- The responder model parameters
        summarization_algorithm -- The summarization algorithm
        summarization_prompt -- The summarization prompt
        summarization_model_id -- The summarization model ID
        summarization_model_params -- The summarization model parameters
        """
        logging.info(f'Requesting information: {goal} {retrieval_requests}')

        try:
            _validate_requests(retrieval_requests)
        except ValueError as e:
            return self.respond(
                body={'message': str(e)},
                status_code=400,
            )

        job = Job(job_type=JobType.INFORMATION_REQUEST)

        jobs = JobsClient()

        jobs.put(job)

        info_request = InformationRequestObj(
            destination_archive_id=destination_archive_id,
            goal=goal,
            job_id=job.job_id,
            job_type=job.job_type,
            include_source_metadata=include_source_metadata,
            retrieval_requests=retrieval_requests,
            responder_use_source_metadata=responder_use_source_metadata,
            responder_model_id=responder_model_id,
            responder_prompt=responder_prompt,
            responder_model_params=responder_model_params,
            summarization_algorithm=summarization_algorithm,
            summarization_prompt=summarization_prompt,
            summarization_model_id=summarization_model_id,
            summarization_model_params=summarization_model_params,
        )

        information_requests = InformationRequestsClient()

        information_requests.put(info_request)

        event_body = InformationRequestBody(
            goal=goal,
            job_id=job.job_id,
            retrieval_requests=retrieval_requests,
            request_id=info_request.request_id,
            resource_names=resource_names,
            responder_model_id=responder_model_id,
            responder_prompt=responder_prompt,
            responder_model_params=responder_model_params,
            summarization_algorithm=summarization_algorithm,
            summarization_prompt=summarization_prompt,
            summarization_model_id=summarization_model_id,
            summarization_model_params=summarization_model_params,
        )

        event = EventBusEvent(
            event_type=InformationRequestBody.event_type,
            body=event_body.to_dict(),
        )

        publisher = EventPublisher()

        publisher.submit(event)

        return self.respond(
            body={
                'job_id': job.job_id,
                'job_type': job.job_type,
                'request_id': info_request.request_id,
            },
            status_code=201,
        )

    def describe_request(self, request_id: str):
        """
        Describe the request 

        Keyword arguments:
        request_id -- The request ID
        """
        information_requests = InformationRequestsClient()

        information_request = information_requests.get(request_id=request_id)

        if not information_request:
            return self.respond(
                body={'message': 'Information request not found'},
                status_code=404,
            )

        response_body = information_request.to_dict(json_compatible=True)

        logging.info(f'Describing request: {response_body}')

        return self.respond(
            body=response_body,
            status_code=200,
        )

    def score_response(self, request_id: str, score: float, score_comment: Optional[str] = None):
        """
        Score the response

        Keyword arguments:
        request_id -- The request ID
        score -- The score
        score_comment -- The score comment
        """
        # TODO: Find the request, validate that a response has been received, and update the score
        requests = InformationRequestsClient()

        request = requests.get(request_id=request_id)

        if not request:
            return self.respond(
                body={'message': 'Information request not found'},
                status_code=404,
            )

        request.response_score = score

        request.response_score_comment = score_comment

        requests.put(request)

        return self.respond(
            body={'message': 'Response scored'},
            status_code=200,
        )