from contextlib import contextmanager
from datetime import datetime, timedelta, UTC as utc_tz
from enum import StrEnum
from uuid import uuid4
from typing import Dict, Generator, Optional

from da_vinci.core.orm import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
    TableScanDefinition,
)


class JobStatus(StrEnum):
    PENDING = 'PENDING'
    IN_PROGRESS = 'IN_PROGRESS'
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'


class Job(TableObject):
    description = 'A job being executed by the system'
    table_name = 'jobs'

    partition_key_attribute = TableObjectAttribute(
        name='job_type',
        attribute_type=TableObjectAttributeType.STRING,
        description='The type of job being executed',
        default='INSIGHT_EXTRACTION'
    )
    
    sort_key_attribute = TableObjectAttribute(
        name='job_id',
        attribute_type=TableObjectAttributeType.STRING,
        description='The unique identifier of the job',
        default=lambda: str(uuid4())
    )

    ttl_attribute = TableObjectAttribute(
        name='time_to_live',
        attribute_type=TableObjectAttributeType.DATETIME,
        description='The time to live of the job',
        optional=True,
        default=lambda: datetime.now(tz=utc_tz) + timedelta(days=2)
    )

    attributes = [
        TableObjectAttribute(
            name='created_on',
            attribute_type=TableObjectAttributeType.DATETIME,
            description='The timestamp of when the job was created',
            default=lambda: datetime.now(tz=utc_tz),
        ),

        TableObjectAttribute(
            name='ended',
            attribute_type=TableObjectAttributeType.DATETIME,
            description='The timestamp of when the job ended',
            optional=True,
            default=None,
        ),

        TableObjectAttribute(
            name='parent_job_id',
            attribute_type=TableObjectAttributeType.STRING,
            description='The parent job identifier',
            optional=True,
            default=None,
        ),

        TableObjectAttribute(
            name='parent_job_type',
            attribute_type=TableObjectAttributeType.STRING,
            description='The parent job type',
            optional=True,
            default=None,
        ),

        TableObjectAttribute(
            name='started',
            attribute_type=TableObjectAttributeType.DATETIME,
            description='The timestamp of when the job started',
            optional=True,
            default=None,
        ),

        TableObjectAttribute(
            name='status',
            attribute_type=TableObjectAttributeType.STRING,
            description='The status of the job being executed',
            default='PENDING',
        ),

        TableObjectAttribute(
            name='status_message',
            attribute_type=TableObjectAttributeType.STRING,
            description='The message of the job status',
            optional=True,
        ),
    ]

    def create_child(self, job_type: str) -> 'Job':
        """
        Creates a child job

        Keyword arguments:
        job_type -- The type of job to create
        """
        child_job = Job(
            job_type=job_type,
            parent_job_id=self.job_id,
            parent_job_type=self.job_type,
        )

        return child_job


class JobsScanDefinition(TableScanDefinition):
    def __init__(self):
        super().__init__(table_object_class=Job)


class JobsClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        """
        Initializes the object

        Keyword arguments:
        app_name -- The name of the application
        deployment_id -- The deployment identifier
        """
        super().__init__(
            app_name=app_name,
            deployment_id=deployment_id,
            default_object_class=Job,
        )

    @contextmanager
    def get_and_update(self, job_type: str, job_id: str) -> Generator[Job, None, None]:
        """
        Gets and updates a job. Built to be used with Python's "with" statement

        Keyword arguments:
        job_type -- The type of job
        job_id -- The job identifier
        """
        job = self.get(job_type, job_id)

        if not job:
            raise ValueError(f'Job {job_id} not found')

        try:
            yield job

        finally:
            self.put(job)

    def delete(self, job: Job) -> None:
        """
        Deletes the job

        Keyword arguments:
        job -- The job to delete
        """
        self.delete_object(job)

    def get(self, job_type: str, job_id: str, consistent_read: bool = False) -> Job:
        """
        Retrieves a job by its identifier

        Keyword arguments:
        job_type -- The type of job
        job_id -- The job identifier
        """
        dynamodb_key = self.default_object_class.gen_dynamodb_key(
            partition_key_value=job_type,
            sort_key_value=job_id,
        )

        results = self.client.get_item(
            TableName=self.table_endpoint_name,
            Key=dynamodb_key,
            ConsistentRead=consistent_read,
        )

        if 'Item' not in results:
            return None

        return self.default_object_class.from_dynamodb_item(results['Item'])

    def fail_parent_job(self, job: Job, failure_status_message: str) -> bool:
        """
        Fails the parent job, returning whether or not the parent job was found

        Keyword arguments:
        job -- The job to fail
        failure_status_message -- The message to set if the job fails
        """
        if job.parent_job_id and job.parent_job_type:
            parent_job = self.get(job_type=job.parent_job_type, job_id=job.parent_job_id)

            parent_job.status = JobStatus.FAILED

            parent_job.status_message = failure_status_message

            self.put(parent_job)

            return True

        return False

    @contextmanager
    def job_execution(self, job: Job, fail_all_parents: Optional[bool] = False, fail_parent: Optional[bool] = False,
                      failure_status_message: Optional[str] = None, skip_completion: Optional[bool] = False,
                      skip_initialization: Optional[bool] = False) -> Generator[Job, None, None]:
        """
        Syntactic sugar to manage a job. Built to be used with Python's "with" statement

        Supports kicking off a job, marking it as failed, and marking it as completed

        Keyword arguments:
        job -- The job to start and end
        fail_all_parents -- Whether or not to fail all parent jobs, work all the way up the chain
        fail_parent -- Whether or not to fail the parent job
        failure_status_message -- The message to set if the job fails
        skip_completion -- Whether or not to skip the completion of the job
        skip_initialization -- Whether or not to skip the initialization of the job
        """
        if not skip_initialization and job.status != JobStatus.IN_PROGRESS:
            job.status = JobStatus.IN_PROGRESS

            job.started = datetime.now(tz=utc_tz)

            self.put(job)

        failed = False

        try:
            yield job

        except Exception as error:
            job.status = JobStatus.FAILED

            job.ended = datetime.now(tz=utc_tz)

            job.status_message = failure_status_message or str(error)

            self.put(job)

            failed = True

            if fail_parent and job.parent_job_id:
                self.fail_parent_job(job, failure_status_message)

            if fail_all_parents:
                parent_job = job

                chain_not_ended = parent_job.parent_job_id is not None

                while chain_not_ended:
                    chain_not_ended = self.fail_parent_job(parent_job, failure_status_message)

            raise

        finally:
            if failed:
                raise

            if not failed and not skip_completion:
                job.status = JobStatus.COMPLETED

                job.ended = datetime.now(tz=utc_tz)

                self.put(job)


    def put(self, job: Job) -> None:
        """
        Puts the job

        Keyword arguments:
        job -- The job to put
        """
        self.put_object(job)