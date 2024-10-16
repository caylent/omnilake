from datetime import datetime, UTC as utc_tz
from typing import List, Optional, Set, Union
from uuid import uuid4

from da_vinci.core.orm import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
)


class VectorStoreQuery(TableObject):
    table_name = 'vector_store_queries'

    description = 'Table tracking all vector store queries'

    partition_key_attribute = TableObjectAttribute(
        name='query_id',
        attribute_type=TableObjectAttributeType.STRING,
        description='The unique identifier for the vector store query.',
        default=lambda: str(uuid4()),
    )

    attributes = [
        TableObjectAttribute(
            name='archive_id',
            attribute_type=TableObjectAttributeType.STRING,
            description='The archive ID for the vector store query.',
            optional=True,
        ),

        TableObjectAttribute(
            name='created_on',
            attribute_type=TableObjectAttributeType.DATETIME,
            description='The date and time the query was created.',
            default=lambda: datetime.now(tz=utc_tz),
        ),

        TableObjectAttribute(
            name='completed_on',
            attribute_type=TableObjectAttributeType.DATETIME,
            description='The date and time the query was completed.',
            optional=True,
        ),

        TableObjectAttribute(
            name='job_id',
            attribute_type=TableObjectAttributeType.STRING,
            description='The unique identifier for the job that the query is associated with.',
            optional=True,
        ),

        TableObjectAttribute(
            name='job_type',
            attribute_type=TableObjectAttributeType.STRING,
            description='The type of job that the query is associated with.',
            optional=True,
        ),

        TableObjectAttribute(
            name='max_entries',
            attribute_type=TableObjectAttributeType.NUMBER,
            description='The maximum number of entries to return.',
            optional=True,
        ),

        TableObjectAttribute(
            name='query',
            attribute_type=TableObjectAttributeType.STRING,
            description='The query for the vector store.',
        ),

        TableObjectAttribute(
            name='remaining_processes',
            attribute_type=TableObjectAttributeType.NUMBER,
            description='The number of processes remaining for the query.',
            default=0,
            optional=True,
        ),

        TableObjectAttribute(
            name='request_id',
            attribute_type=TableObjectAttributeType.STRING,
            description='The Information Request ID that this query is associated with.',
        ),

        TableObjectAttribute(
            name='resulting_resources',
            attribute_type=TableObjectAttributeType.STRING_SET,
            description='The result of the query.',
            default=set(),
            optional=True,
        ),

        TableObjectAttribute(
            name='target_tags',
            attribute_type=TableObjectAttributeType.STRING_LIST,
            description='The expected tags to rank the vector stores and results against.',
            default=[],
        ),

        TableObjectAttribute(
            name='vector_store_ids',
            attribute_type=TableObjectAttributeType.STRING_LIST,
            description='The unique identifier for the vector stores that are being queried.',
            default=[],
        ),
    ]


class VectorStoreQueryClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        super().__init__(app_name=app_name, default_object_class=VectorStoreQuery, deployment_id=deployment_id)

    def add_resulting_resources(self, query_id: str, resulting_resources: Union[List, Set], decrement_process: Optional[bool] = True) -> None:
        """
        Add resulting resources to a vector store query.

        Keyword arguments:
        decrement_process -- Whether or not to decrement the remaining processes (default: {True})
        query_id -- The unique identifier for the vector store query.
        """
        update_expression = "ADD ResultingResources :resource_name_set"

        expression_attribute_values = {
            ':resource_name_set': {'SS': list(resulting_resources)},
        }

        if decrement_process:
            update_expression += " SET RemainingProcesses = if_not_exists(RemainingProcesses, :start) - :decrement"

            expression_attribute_values.update({
                ':decrement': {'N': "1"},
                ':start': {'N': "0"},
            })

        self.client.update_item(
            TableName=self.table_endpoint_name,
            Key={
                'QueryId': {'S': query_id},
            },
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
        )

    def get(self, query_id: str, consistent_read: Optional[bool] = False) -> Union[VectorStoreQuery, None]:
        """
        Get a vector store query by its unique identifier.

        Keyword arguments:
        query_id -- The unique identifier for the vector store query.
        consistent_read -- Whether or not to perform a consistent read (default: {False})
        """
        return self.get_object(partition_key_value=query_id, consistent_read=consistent_read)

    def put(self, query: VectorStoreQuery) -> None:
        """
        Put a vector store query.

        Keyword arguments:
        query -- The vector store query to put.
        """
        return self.put_object(table_object=query)