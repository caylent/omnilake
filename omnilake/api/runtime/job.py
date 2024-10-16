'''
Handles the job API
'''
from omnilake.api.runtime.construct import ChildAPI, Route

from omnilake.tables.jobs.client import JobsClient


class JobsAPI(ChildAPI):
    routes = [
        Route(
            path='/describe_job',
            method_name='describe_job',
        ),
    ]

    def describe_job(self, job_type: str, job_id: str):
        """
        Describe a job

        Keyword arguments:
        job_type -- The job type
        job_id -- The job ID
        """

        jobs = JobsClient()

        job = jobs.get(job_type=job_type, job_id=job_id)

        if not job:
            return self.respond(
                body='Job not found',
                status_code=404,
            )

        return self.respond(
            body=job.to_dict(json_compatible=True),
            status_code=200,
       )