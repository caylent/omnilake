from datetime import datetime, UTC as utc_tz
from enum import StrEnum
from hashlib import sha256
from typing import Dict, List, Optional, Union
from uuid import uuid4

from da_vinci.core.orm import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
    TableScanDefinition,
)


class CoordinatedLakeRequestStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class CoordinatedLakeRequestValidationStatus(StrEnum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"


class LakeChainCoordinatedLakeRequest(TableObject):
    table_name = "lake_chain_coordinated_lake_requests"

    description = "Tracks all of the lake requests that a chain is waiting on."

    partition_key_attribute = TableObjectAttribute(
        name="chain_request_id",
        attribute_type=TableObjectAttributeType.STRING,
        description="The unique identifier for the request chain.",
    )

    sort_key_attribute = TableObjectAttribute(
        name="lake_request_id",
        attribute_type=TableObjectAttributeType.STRING,
        description="The unique identifier for the request",
    )

    attributes = [
        TableObjectAttribute(
            name="chain_request_name",
            attribute_type=TableObjectAttributeType.STRING,
            description="The unique name for the request.",
        ),

        TableObjectAttribute(
            name="created_on",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The date and time the request was created.",
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name="execution_status",
            attribute_type=TableObjectAttributeType.STRING,
            description="The status of the request.",
            default=CoordinatedLakeRequestStatus.PENDING,
        ),

        TableObjectAttribute(
            name="validation_instructions",
            attribute_type=TableObjectAttributeType.STRING,
            description="The instructions for validating the request.",
            optional=True,
        ),

        TableObjectAttribute(
            name="validation_model_id",
            attribute_type=TableObjectAttributeType.STRING,
            description="The model id for the validation instructions.",
            optional=True,
        ),

        TableObjectAttribute(
            name="validation_status",
            attribute_type=TableObjectAttributeType.STRING,
            description="The results of the request validation. Either SUCCESS or FAILURE.",
            optional=True,
        ),
    ]

    def __init__(self, chain_request_name: str, chain_request_id: str, lake_request_id: str,
                 created_on: Optional[datetime] = None, execution_status: Optional[Union[CoordinatedLakeRequestStatus, str]] = None,
                 validation_instructions: Optional[str] = None, validation_model_id: Optional[str] = None,
                 validation_status: Optional[Union[CoordinatedLakeRequestValidationStatus, str]] = None):
        """
        Initialize a LakeRequestChainRunningRequest object.

        Keyword Arguments:
        chain_request_id -- The unique identifier for the request chain.
        chain_request_name -- The unique name for the request.
        created_on -- The date and time the request was created.
        lake_request_id -- The unique identifier for the request
        execution_status -- The status of the request.
        validation_instructions -- The instructions for validating the request.
        validation_model_id -- The model id for the validation instructions.
        validation_status -- The results of the request validation. Either SUCCESS or FAILURE.
        """
        super().__init__(
            chain_request_id=chain_request_id,
            chain_request_name=chain_request_name,
            created_on=created_on,
            lake_request_id=lake_request_id,
            execution_status=execution_status,
            validation_instructions=validation_instructions,
            validation_model_id=validation_model_id,
            validation_status=validation_status,
        )


class LakeChainCoordinatedLakeRequestScanDefinition(TableScanDefinition):
    def __init__(self):
        super().__init__(table_object_class=LakeChainCoordinatedLakeRequest)


class LakeChainCoordinatedLakeRequestsClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        super().__init__(
            app_name=app_name,
            default_object_class=LakeChainCoordinatedLakeRequest,
            deployment_id=deployment_id,
        )

    def get(self, chain_request_id: str, lake_request_id: str) -> Optional[LakeChainCoordinatedLakeRequest]:
        """
        Get a LakeRequestChainPending object by its lake_request_id.

        Keyword Arguments:
        lake_request_id -- The lake_request_id to get.
        """
        return self.get_object(
            partition_key_value=chain_request_id,
            sort_key_value=lake_request_id,
        )

    def get_by_lake_request_id(self, lake_request_id: str) -> Optional[LakeChainCoordinatedLakeRequest]:
        """
        Get a LakeRequestChainPending object by its lake_request_id.

        Keyword Arguments:
        lake_request_id -- The lake_request_id to get.
        """
        params = {
            "KeyConditionExpression": "#k = :v",
            "ExpressionAttributeNames": {"#k": "LakeRequestId"},
            "ExpressionAttributeValues": {":v": {"S": lake_request_id}},
            "IndexName": "lake_request_id_index",
            "Select": "ALL_ATTRIBUTES",
            "TableName": self.table_endpoint_name,
        }

        result = self.client.query(**params)

        if result['Count'] == 0:
            return None
        else:
            return self.default_object_class.from_dynamodb_item(
                result['Items'][0]
            )

    def get_all_by_chain_request_id(self, chain_request_id: str) -> List[LakeChainCoordinatedLakeRequest]:
        """
        Get all LakeRequestChainPending objects by their chain_request_id.

        Keyword Arguments:
        chain_request_id -- The chain_request_id to get.
        """
        params = {
            "KeyConditionExpression": "ChainRequestId = :r_id",
            "ExpressionAttributeValues": {
                ":r_id": {"S": chain_request_id},
            }
        }

        all_coordinated_requests = []

        for page in self.paginated(parameters=params):
            all_coordinated_requests.extend(page)

        return all_coordinated_requests

    def delete(self, running_request: LakeChainCoordinatedLakeRequest) -> None:
        """
        Delete a running request object.

        Keyword Arguments:
        lake_request_id -- The lake_request_id to delete.
        """
        self.delete_object(running_request)

    def put(self, running_request: LakeChainCoordinatedLakeRequest) -> None:
        """
        Put a LakeRequestChainPending object.

        Keyword Arguments:
        pending_request -- The LakeRequestChainPending object to put.
        """
        self.put_object(running_request)