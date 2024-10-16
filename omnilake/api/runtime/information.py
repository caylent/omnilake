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

from omnilake.tables.entries.client import EntriesClient
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
    required_keys = ['archive_id', 'evaluation_type', 'request_type', 'max_entries']

    for key in required_keys:
        if key not in request:
            raise ValueError(f'Missing required key: {key}')

    evaluation_type = request['evaluation_type'].upper()

    if evaluation_type not in ['INCLUSIVE', 'EXCLUSIVE']:
        raise ValueError('Invalid evaluation type')

    if evaluation_type == 'INCLUSIVE':
        additional_keys = ['sample_size_percentage']
    else:
        additional_keys = []

    for key in additional_keys:
        if key not in request:
            raise ValueError(f'Missing required key for "INCLUSIVE" requests: {key}')

    request_type = request['request_type'].upper()
    
    if request_type not in ['BASIC', 'VECTOR']:
        raise ValueError('Invalid request type')

    if request_type == 'VECTOR':
        if 'query_string' not in request:
            raise ValueError('Missing required key for "VECTOR" requests: query_string')


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
            path='/request_information',
            method_name='request_information',
        ),
        Route(
            path='/describe_request',
            method_name='describe_request',
        )
    ]

    def request_information(self, goal: str, requests: List[Dict], destination_archive_id: Optional[str] = None, 
                            resource_names: Optional[List[str]] = None):
        """
        Request the system to provide information

        Keyword arguments:
        goal -- The goal of the request
        requests -- The requests
        destination_archive_id -- The destination archive ID, optional
        resource_names -- The resource names
        """
        logging.info(f'Requesting information: {goal} {requests} {destination_archive_id} {resource_names}')

        try:
            _validate_requests(requests)
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
            requests=requests,
        )

        information_requests = InformationRequestsClient()

        information_requests.put(info_request)

        event_body = InformationRequestBody(
            goal=goal,
            job_id=job.job_id,
            requests=requests,
            request_id=info_request.request_id,
            resource_names=resource_names,
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