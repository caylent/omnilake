from datetime import datetime, timedelta, UTC as utc_tz
from typing import Optional, Union
from uuid import uuid4

from botocore.exceptions import ClientError

from da_vinci.core.orm import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
)


class MutexLock(TableObject):
    table_name = "inception_mutex"
    description = "Tracks all of the mutex locks for the inception processor."

    partition_key_attribute = TableObjectAttribute(
        name="lake_request_id",
        attribute_type=TableObjectAttributeType.STRING,
        description="The lake request identifier of the executing inception process for the lock.",
    )

    ttl_attribute = TableObjectAttribute(
        name="time_to_live",
        attribute_type=TableObjectAttributeType.DATETIME,
        description="The date and time the entry will expire.",
        default=lambda: datetime.now(utc_tz) + timedelta(days=1),
        optional=True,
    )

    attributes = [
        TableObjectAttribute(
            name="created_on",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The date and time the entry was created.",
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name="lock_id",
            attribute_type=TableObjectAttributeType.STRING,
            description="The unique identifier for the lock.",
            default=lambda: str(uuid4()),
        ),
    ]

    def __init__(self, lake_request_id: str, created_on: Optional[datetime] = None, lock_id: Optional[str] = None,
                 time_to_live: Optional[datetime] = None):
        """
        Initialize a new lock object.

        Keyword arguments:
        lake_request_id -- The lake request identifier of the executing inception process for the lock.
        created_on -- The date and time the entry was created.
        lock_id -- The unique identifier for the lock.
        """

        super().__init__(
            lake_request_id=lake_request_id,
            created_on=created_on,
            lock_id=lock_id,
            time_to_live=time_to_live,
        )


class InceptionMutexClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        super().__init__(
            app_name=app_name,
            deployment_id=deployment_id,
            default_object_class=MutexLock,
        )

    def get(self, lake_request_id: str, consistent_read: Optional[bool] = False) -> Union[MutexLock, None]:
        """
        Get a lock for the inception processor.

        Keyword arguments:
        lake_request_id -- The lake request identifier of the executing inception process for the lock.
        """
        return self.get_object(partition_key_value=lake_request_id, consistent_read=consistent_read)

    def put(self, lock: MutexLock) -> Union[MutexLock, None]:
        """
        Put a lock for the inception processor.

        Keyword arguments:
        lock -- The lock object to put.
        """
        return self.put_object(lock)

    def request_lock(self, lake_request_id: str) -> Union[str, None]:
        """
        Request a lock for the inception processor. Returns True if the lock was successfully acquired.

        Keyword arguments:
        lake_request_id -- The lake request identifier of the executing inception process for the lock.
        """
        lock = MutexLock(lake_request_id=lake_request_id)

        lock_id = lock.lock_id
        
        try:
            # Use a conditional write with boto3
            self.client.put_item(
                TableName=self.table_endpoint_name,
                Item=lock.to_dynamodb_item(),
                ConditionExpression="attribute_not_exists(lake_request_id)"
            )

            return lock_id

        except ClientError as e:
            # Check if this is a ConditionalCheckFailedException
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':

                # The lock already exists
                return None

            raise

    def validate_lock(self, lake_request_id: str, lock_id: str) -> bool:
        """
        Validate a lock for the inception processor. Returns True if the lock is valid.

        Keyword arguments:
        lake_request_id -- The lake request identifier of the executing inception process for the lock.
        lock_id -- The unique identifier for the lock.
        """
        lock = self.get(lake_request_id=lake_request_id, consistent_read=True)

        if lock is None:
            return False

        return lock.lock_id == lock_id