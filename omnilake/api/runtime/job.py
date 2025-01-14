"""
Handles the job API
"""
from da_vinci.core.immutable_object import (
    ObjectBody,
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)

from omnilake.api.runtime.base import ChildAPI, Route

from omnilake.tables.jobs.client import JobsClient


class DescribeJobSchema(ObjectBodySchema):
    attributes = [
        SchemaAttribute(
            name='job_type',
            type=SchemaAttributeType.STRING,
        ),

        SchemaAttribute(
            name='job_id',
            type=SchemaAttributeType.STRING,
        ),
    ]


class JobsAPI(ChildAPI):
    routes = [
        Route(
            path='/describe_job',
            method_name='describe_job',
            request_body_schema=DescribeJobSchema,
        ),
    ]

    def describe_job(self, request_body: ObjectBody):
        """
        Describe a job

        Keyword arguments:
        request_body -- the request body
        """
        job_type = request_body["job_type"]

        job_id = request_body["job_id"]

        jobs = JobsClient()

        job = jobs.get(job_type=job_type, job_id=job_id)

        if not job:
            return self.respond(
                body={'message': 'job not found'},
                status_code=404,
            )

        return self.respond(
            body=job.to_dict(json_compatible=True),
            status_code=200,
       )