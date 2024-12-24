from datetime import datetime, UTC as utc_tz
from enum import StrEnum
from hashlib import sha256
from typing import List, Optional, Set, Union
from uuid import uuid4

from da_vinci.core.orm import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
    TableScanDefinition,
)


class InformationRequestType(StrEnum):
    INCLUSIVE = 'INCLUSIVE'
    EXCLUSIVE = 'EXCLUSIVE'


class InformationRequestStatus(StrEnum):
    COMPLETED = 'COMPLETED'
    FAILED = 'FAILED'
    PENDING = 'PENDING'
    PROCESSING = 'PROCESSING'


class InformationRequest(TableObject):
    table_name = 'information_requests'

    description = 'Table tracking all external information requests'

    partition_key_attribute = TableObjectAttribute(
        name='request_id',
        attribute_type=TableObjectAttributeType.STRING,
        description='The unique identifier for the information request. Mathes the Job ID of the parent job processing the request',
        default=lambda: str(uuid4()),
    )

    attributes = [
        TableObjectAttribute(
            name='destination_archive_id',
            attribute_type=TableObjectAttributeType.STRING,
            description='The destination archive ID for the information request.',
            optional=True,
        ),

        TableObjectAttribute(
            name='entry_id',
            attribute_type=TableObjectAttributeType.STRING,
            description='The resulting entry generated by the information request.',
            optional=True,
        ),

        TableObjectAttribute(
            name='goal',
            attribute_type=TableObjectAttributeType.STRING,
            description='The goal of the information request.',
        ),

        TableObjectAttribute(
            name='job_id',
            attribute_type=TableObjectAttributeType.STRING,
            description='The job ID of the parent job processing the request.',
        ),

        TableObjectAttribute(
            name='job_type',
            attribute_type=TableObjectAttributeType.STRING,
            description='The job type of the parent job processing the request.',
        ),

        TableObjectAttribute(
            name='include_source_metadata',
            attribute_type=TableObjectAttributeType.BOOLEAN,
            description='Whether to include source metadata in the response.',
            optional=True,
            default=False,
        ),

        TableObjectAttribute(
            name='original_sources',
            attribute_type=TableObjectAttributeType.STRING_SET,
            description='The original sources of the information request.',
            optional=True,
        ),

        TableObjectAttribute(
            name='remaining_queries',
            attribute_type=TableObjectAttributeType.NUMBER,
            description='The number of queries remaining for the information request.',
            default=0,
            optional=True,
        ),

        TableObjectAttribute(
            name='retrieval_requests',
            attribute_type=TableObjectAttributeType.JSON_STRING_LIST,
            description='The information retrieval requests used to fetch information for assisting the user with their goal.',
        ),

        TableObjectAttribute(
            name='requested_on',
            attribute_type=TableObjectAttributeType.DATETIME,
            description='The date and time the information request was made.',
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name='request_status',
            attribute_type=TableObjectAttributeType.STRING,
            description='The status of the information request.',
            default=InformationRequestStatus.PENDING,
        ),

        TableObjectAttribute(
            name='response_completed_on',
            attribute_type=TableObjectAttributeType.DATETIME,
            description='The date and time the information response was completed.',
            optional=True,
        ),

        TableObjectAttribute(
            name='response_score',
            attribute_type=TableObjectAttributeType.NUMBER,
            description='The score of the response.',
            optional=True,
            default=0,
        ),

        TableObjectAttribute(
            name='response_score_comment',
            attribute_type=TableObjectAttributeType.STRING,
            description='The comment on the response score.',
            optional=True,
        ),

        TableObjectAttribute(
            name='responder_model_id',
            attribute_type=TableObjectAttributeType.STRING,
            description='The model ID used by the final responder.',
            optional=True,
        ),

        TableObjectAttribute(
            name='responder_model_params',
            attribute_type=TableObjectAttributeType.JSON_STRING,
            description='The parameters used for the responder model',
            optional=True,
            default={},
        ),

        TableObjectAttribute(
            name='responder_prompt',
            attribute_type=TableObjectAttributeType.STRING,
            description='The prompt used by the final responder.',
            optional=True,
        ),

        TableObjectAttribute(
            name='summarization_algorithm',
            attribute_type=TableObjectAttributeType.STRING,
            description='The summarization algorithm used for the information request.',
            optional=True,
        ),

        TableObjectAttribute(
            name='summarization_model_id',
            attribute_type=TableObjectAttributeType.STRING,
            description='The model ID used for summarization system',
            optional=True,
        ),

        TableObjectAttribute(
            name='summarization_prompt',
            attribute_type=TableObjectAttributeType.STRING,
            description='The prompt used for summarization system',
            optional=True,
        ),

        TableObjectAttribute(
            name='summarization_model_params',
            attribute_type=TableObjectAttributeType.JSON_STRING,
            description='The parameters used for the summarization model',
            optional=True,
            default={},
        ),
    ]


class InformationRequestsScanDefinition(TableScanDefinition):
    def __init__(self):
        super().__init__(table_object_class=InformationRequest)


class InformationRequestsClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        """
        Initialize the Information Requests Client

        Keyword Arguments:
            app_name -- The name of the app.
            deployment_id -- The deployment ID.
        """
        super().__init__(
            app_name=app_name,
            deployment_id=deployment_id,
            default_object_class=InformationRequest,
        )

    def add_query_results(self, request_id: str, results: Union[List, Set]) -> int:
        """
        Adds a completed resource to a compaction job context

        Keyword arguments:
        decrement_process -- Whether or not to decrement the remaining processes (default: {True})
        request_id -- The request ID of the compaction job context
        resource_name -- The name of the resource to add
        """
        update_expression = "ADD OriginalSources :results SET RemainingQueries = if_not_exists(RemainingQueries, :start) - :decrement"

        expression_attribute_values = {
            ':decrement': {'N': "1"},
            ':results': {'SS': list(results)},
            ':start': {'N': "0"},
        }

        response = self.client.update_item(
            TableName=self.table_endpoint_name,
            Key={
                'RequestId': {'S': request_id},
            },
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues='UPDATED_NEW',
        )

        updated_remaining_queries = int(response['Attributes']['RemainingQueries']['N'])
    
        return updated_remaining_queries

    def delete(self, information_request: InformationRequest) -> None:
        """
        Delete an information request object from the table

        Keyword Arguments:
            information_request -- The information request object.
        """
        return self.delete_object(information_request)

    def get(self, request_id: str, consistent_read: Optional[bool] = False) -> Union[InformationRequest, None]:
        """
        Get an information request by request ID

        Keyword Arguments:
            request_id -- The request ID of the information request.

        Returns:
            The information request object.
        """
        return self.get_object(request_id, consistent_read=consistent_read)

    def put(self, information_request: InformationRequest) -> None:
        """
        Put an information request object into the table

        Keyword Arguments:
            information_request -- The information request object.
        """
        return self.put_object(information_request)