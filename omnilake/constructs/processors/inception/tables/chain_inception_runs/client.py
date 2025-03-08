from datetime import datetime, UTC as utc_tz
from enum import StrEnum
from hashlib import sha256
from typing import List, Optional, Union

from da_vinci.core.orm import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
)


class InceptionExecutionStatus(StrEnum):
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    IN_PROGRESS = "IN_PROGRESS"


# 2 chainz
class ChainInceptionRun(TableObject):
    table_name = "chain_inception_runs"
    description = "Tracks the inception runs of chains."

    partition_key_attribute = TableObjectAttribute(
        name="lake_request_id",
        attribute_type=TableObjectAttributeType.STRING,
        description="The ID of the lake request, this chain run is executing within.",
    )

    sort_key_attribute = TableObjectAttribute(
        name="chain_request_id",
        attribute_type=TableObjectAttributeType.STRING,
        description="The ID of the lake chain run.",
    )

    attributes = [
        TableObjectAttribute(
            name="created_on",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The date and time the entry was created.",
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name="execution_status",
            attribute_type=TableObjectAttributeType.STRING,
            description="The status of the chain run.",
            default=InceptionExecutionStatus.IN_PROGRESS,
        ),

        TableObjectAttribute(
            name="job_id",
            attribute_type=TableObjectAttributeType.STRING,
            description="The ID of the job that was kicked off.",
        ),

        TableObjectAttribute(
            name="job_type",
            attribute_type=TableObjectAttributeType.STRING,
            description="The type of job that was kicked off.",
        ),
    ]

    def __init__(self, chain_request_id: str, lake_request_id: str, created_on: Optional[datetime] = None,
                 execution_status: Optional[InceptionExecutionStatus] = None, job_id: Optional[str] = None,
                 job_type: Optional[str] = None):
        """
        Initialize a ChainInceptionRun object.

        Keyword arguments:
        chain_request_id -- The ID of the lake chain run.
        lake_request_id -- The ID of the lake request, this chain run is executing within.
        created_on -- The date and time the entry was created.
        execution_status -- The status of the chain run.
        job_id -- The ID of the job that was kicked off.
        job_type -- The type of job that was kicked off.
        """
        super().__init__(
            chain_request_id=chain_request_id,
            lake_request_id=lake_request_id,
            created_on=created_on,
            execution_status=execution_status,
            job_id=job_id,
            job_type=job_type,
        )

    @staticmethod
    def calculate_hash(content: str) -> str:
        """
        Generate a hash of the content of the entry.

        Keyword arguments:
        content -- The content of the entry.
        """
        content_hash = sha256(content.encode('utf-8'))

        return content_hash.hexdigest()


class ChainInceptionRunClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        super().__init__(
            app_name=app_name,
            deployment_id=deployment_id,
            default_object_class=ChainInceptionRun,
        )

    def all_by_lake_request_id(self, lake_request_id: str, filter_status: Optional[InceptionExecutionStatus] = None) -> List[ChainInceptionRun]:
        """
        Retrieve all runs in the system by lake request ID.

        Keyword arguments:
        lake_request_id -- The ID of the lake request.
        filter_status -- The status to filter by.
        """
        params = {
            "KeyConditionExpression": "LakeRequestId = :lake_rq",
            "ExpressionAttributeValues":{
                ":lake_rq": {"S": lake_request_id},
            },
        }

        if filter_status:
            params["FilterExpression"] = "ExecutionStatus = :status"

            params["ExpressionAttributeValues"][":status"] = {"S": filter_status.value}

        results = []

        for page in self.paginated(call="query", parameters=params):
            results.extend(page)

        return results

    def delete(self, run: ChainInceptionRun) -> None:
        """
        Delete a run from the system.
        """
        return self.delete_object(run)

    def get(self, lake_request_id: str, chain_request_id: str) -> Union[ChainInceptionRun, None]:
        """
        Get a run from the system.

        Keyword arguments:
        chain_request_id -- The ID of the lake chain run.
        """
        return self.get_object(partition_key_value=lake_request_id, sort_key_value=chain_request_id)

    def get_by_chain_request_id(self, chain_request_id: str) -> Union[ChainInceptionRun, None]:
        """
        Get a run from the system by chain request ID.

        Keyword arguments:
        chain_request_id -- The ID of the lake chain request.
        """
        params = {
            "KeyConditionExpression": "ChainRequestId = :chain_rq",
            "ExpressionAttributeValues":{
                ":chain_rq": {"S": chain_request_id},
            },
            "IndexName": "chain-request-id-index",
        }

        results = []

        for page in self.paginated(call="query", parameters=params):
            results.extend(page)

        if len(results) == 0:
            return None

        return results[0]

    def put(self, chain_inception_run: ChainInceptionRun) -> None:
        """
        Put an entry into the system.

        Keyword arguments:
        entry -- The entry to put into the system.
        """
        return self.put_object(chain_inception_run)