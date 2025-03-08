from datetime import datetime, UTC as utc_tz
from enum import StrEnum
from typing import Dict, List, Optional, Set, Union
from uuid import uuid4

from botocore.exceptions import ClientError as DynamoDBClientError

from da_vinci.core.orm import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
    TableScanDefinition,
)


class LakeChainRequestStatus(StrEnum):
    COMPLETED = 'COMPLETED'
    EXECUTING = 'EXECUTING'
    FAILED = 'FAILED'
    PENDING = 'PENDING'


class LakeChainRequest(TableObject):
    table_name = "lake_chain_requests"

    description = "Tracks all of the lake chain requests in the system."

    partition_key_attribute = TableObjectAttribute(
        name="chain_request_id",
        attribute_type=TableObjectAttributeType.STRING,
        description="The unique identifier for the request chain.",
        default=lambda: str(uuid4()),
    )

    attributes = [
        TableObjectAttribute(
            name="callback_event_type",
            attribute_type=TableObjectAttributeType.STRING,
            description="The event type to trigger the callback.",
            optional=True,
        ),

        TableObjectAttribute(
            name="chain",
            attribute_type=TableObjectAttributeType.JSON_STRING_LIST,
            description="The raw representation of a request chain.",
        ),


        TableObjectAttribute(
            name="chain_execution_status",
            attribute_type=TableObjectAttributeType.STRING,
            description="The status of the request chain.",
            default=LakeChainRequestStatus.PENDING,
        ),

        TableObjectAttribute(
            name="conditions_met_requests",
            attribute_type=TableObjectAttributeType.STRING_SET,
            description="The list of request names that are conditional but the conditions have been met.",
            optional=True,
            default=set(),
        ),

        TableObjectAttribute(
            name="created_on",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The date and time the request chain was created.",
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name="ended",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The date and time the request chain ended.",
            optional=True,
        ),

        TableObjectAttribute(
            name="executed_requests",
            attribute_type=TableObjectAttributeType.JSON,
            description="The dictionary of request names/ids that have already been executed in the request chain.",
            optional=True,
            default={},
        ),

        TableObjectAttribute(
            name="job_id",
            attribute_type=TableObjectAttributeType.STRING,
            description="The job ID of the request chain.",
        ),

        TableObjectAttribute(
            name="job_type",
            attribute_type=TableObjectAttributeType.STRING,
            description="The type of job that the request chain is associated with.",
        ),

        TableObjectAttribute(
            name="num_remaining_running_requests",
            attribute_type=TableObjectAttributeType.NUMBER,
            description="The number of requests that are currently running in the request chain.",
            default=0,
            optional=True,
        ),

        TableObjectAttribute(
            name="started",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The date and time the request chain started.",
            optional=True,
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name="unexecuted_request_names",
            attribute_type=TableObjectAttributeType.STRING_SET,
            description="The list of request names that were not executed. This is populated at the conclusion of the request chain.",
            optional=True,
            default=set(),
        ),
    ]

    def __init__(self, chain: List[Dict],  job_id: str, job_type: str, callback_event_type: Optional[str] = None,
                 chain_execution_status: Optional[str] = None, chain_request_id: Optional[str] = None,
                 conditions_met_requests: Optional[List[str]] = None, created_on: Optional[datetime] = None,
                 ended: Optional[datetime] = None, executed_requests: Optional[Dict] = None,
                 num_remaining_running_requests: Optional[int] = 0, started: Optional[datetime] = None,
                 unexecuted_request_names: Optional[Set[str]] = None):
            """
            Initialize a LakeChainRequest object.
    
            Keyword Arguments:
            callback_event_type -- The event type to trigger the callback.
            chain -- The raw representation of a request chain.
            chain_execution_status -- The status of the request chain.
            chain_request_id -- The unique identifier for the request chain.
            conditions_met_requests -- The list of request names that are conditional but the conditions have been met.
            created_on -- The date and time the request chain was created.
            ended -- The date and time the request chain ended.
            executed_requests -- The dictionary of request names/ids that have already been executed in the request chain.
            job_id -- The job ID of the request chain.
            job_type -- The type of job that the request chain is associated with.
            num_remaining_running_requests -- The number of requests that are currently running in the request chain.
            started -- The date and time the request chain started.
            unexecuted_request_names -- The list of request names that were not executed. This is populated at the conclusion of the request chain.
            """
            super().__init__(
                callback_event_type=callback_event_type,
                chain=chain,
                chain_execution_status=chain_execution_status,
                chain_request_id=chain_request_id,
                conditions_met_requests=conditions_met_requests,
                created_on=created_on,
                ended=ended,
                executed_requests=executed_requests,
                job_id=job_id,
                job_type=job_type,
                num_remaining_running_requests=num_remaining_running_requests,
                started=started,
                unexecuted_request_names=unexecuted_request_names,
            )


class LakeChainRequestsScanDefinition(TableScanDefinition):
    def __init__(self):
        super().__init__(table_object_class=LakeChainRequest)


class LakeChainRequestsClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        """
        Initialize the lake requests chains Client

        Keyword Arguments:
            app_name -- The name of the app.
            deployment_id -- The deployment ID.
        """
        super().__init__(
            app_name=app_name,
            deployment_id=deployment_id,
            default_object_class=LakeChainRequest,
        )

    def add_condition_met_request(self, chain_request_id: str, request_name: str) -> None:
        """
        Add a request to the list of conditions met requests

        Keyword arguments:
        chain_request_id -- The ID of the request chain.
        request_name -- The name of the request.
        """
        update_expression = "ADD ConditionsMetRequests :request_name"

        expression_attribute_values = {
            ':request_name': {'SS': [request_name]},
        }

        self.client.update_item(
            TableName=self.table_endpoint_name,
            Key={
                'ChainRequestId': {'S': chain_request_id},
            },
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
        )

    def increment_remaining_running_requests(self, chain_request_id: str, increment_by: int = 1) -> int:
        """
        Increment the number of remaining running requests

        Keyword arguments:
        chain_request_id -- The ID of the request chain.
        """
        update_expression = "SET NumRemainingRunningRequests = if_not_exists(NumRemainingRunningRequests, :start) + :increment"

        expression_attribute_values = {
            ':increment': {'N': str(increment_by)},
            ':start': {'N': "0"},
        }

        response = self.client.update_item(
            TableName=self.table_endpoint_name,
            Key={
                'ChainRequestId': {'S': chain_request_id},
            },
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues='UPDATED_NEW',
        )

        updated_remaining_requests = int(response['Attributes']['NumRemainingRunningRequests']['N'])

        return updated_remaining_requests

    def record_lake_request_results(self, chain_request_id: str, lake_request_id: str, reference_name: str) -> int:
        """
        Add lookup results to the lake request

        Keyword arguments:
        chain_request_id -- The ID of the request chain.
        lake_request_id -- The ID of the lake request.
        reference_name -- The name of the request.
        """
        counter_expression = "NumRemainingRunningRequests = if_not_exists(NumRemainingRunningRequests, :start) - :decrement"

        obj_expression = "#executedRequests.#key = :value"

        update_expression = f"SET {obj_expression}, {counter_expression}"

        expression_attr_names = {
            "#executedRequests": "ExecutedRequests",
            "#key": reference_name,
        }

        try:
            response = self.client.update_item(
                TableName=self.table_endpoint_name,
                Key={
                    "ChainRequestId": {"S": chain_request_id},
                },
                UpdateExpression=update_expression,
                ExpressionAttributeNames=expression_attr_names,
                ExpressionAttributeValues={
                    ":decrement": {"N": "1"},
                    ":start": {"N": "0"},
                    ":value": {"S": lake_request_id},
                },
                ReturnValues="UPDATED_NEW",
            )
        except DynamoDBClientError as dyn_err:
            if "The document path provided in the update expression is invalid for update" in str(dyn_err):
                retry_obj_expression = "ExecutedRequests = :map"

                retry_update_expression = f"SET {retry_obj_expression}, {counter_expression}"

                response = self.client.update_item(
                    TableName=self.table_endpoint_name,
                    Key={
                        "ChainRequestId": {"S": chain_request_id},
                    },
                    UpdateExpression=retry_update_expression,
                    ExpressionAttributeValues={
                        ":map": {"M": {reference_name: {"S": lake_request_id}}},
                        ":start": {"N": "0"},
                        ":decrement": {"N": "1"}
                    },
                    ReturnValues="UPDATED_NEW",
                )

            else:
                raise

        updated_remaining_requests = int(response["Attributes"]["NumRemainingRunningRequests"]["N"])
    
        return updated_remaining_requests

    def delete(self, request_chain: LakeChainRequest) -> None:
        """
        Delete an lake request chain object from the table

        Keyword Arguments:
            lake_request_chain -- The lake request chain object.
        """
        return self.delete_object(request_chain)

    def get(self, chain_request_id: str, consistent_read: Optional[bool] = False) -> Union[LakeChainRequest, None]:
        """
        Get an lake request chain object from the table

        Keyword Arguments:
            lake_chain_request_id -- The lake chain request id.
            consistent_read -- Whether or not to use consistent read.
        """
        return self.get_object(partition_key_value=chain_request_id, consistent_read=consistent_read)

    def put(self, chain_request: LakeChainRequest) -> None:
        """
        Put an lake request chain object into the table

        Keyword Arguments:
            lake_request_chain -- The lake request chain object.
        """
        return self.put_object(chain_request)

    def put_if_not_exists(self, chain_request: LakeChainRequest) -> bool:
        """
        Put an lake request chain object into the table if it does not already exist

        Keyword Arguments:
            lake_request_chain -- The lake request chain object.
        """

        try:
            # Use a conditional write with boto3
            self.client.put_item(
                TableName=self.table_endpoint_name,
                Item=chain_request.to_dynamodb_item(),
                ConditionExpression="attribute_not_exists(lake_request_id)"
            )

            return True

        except DynamoDBClientError as e:
            # Check if this is a ConditionalCheckFailedException
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':

                # Already exists
                return False

            raise