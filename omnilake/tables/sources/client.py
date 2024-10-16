from datetime import datetime, UTC as utc_tz
from enum import StrEnum
from typing import Optional, Union
from uuid import uuid4

from da_vinci.core.orm import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
    TableScanDefinition,
)


class SourceCategory(StrEnum):
    """
    Source Category
    """
    ARTICLE = 'ARTICLE'
    BOOK = 'BOOK'
    INTERNAL_KNOWLEDGE = 'INTERNAL_KNOWLEDGE'
    MEDIA = 'MEDIA'
    PERSONAL_COMMUNICATION = 'PERSONAL_COMMUNICATION'
    REPORT = 'REPORT'
    WEBSITE = 'WEBSITE'


class Source(TableObject):
    table_name = 'sources'

    description = 'Cortex Cited Data Sources'

    partition_key_attribute = TableObjectAttribute(
        name='source_category',
        attribute_type=TableObjectAttributeType.STRING,
        description='The category of the source. (e.g. ARTICLE, BOOK, INTERNAL_KNOWLEDGE, MEDIA, PERSONAL_COMMUNICATION, REPORT, WEBSITE)',
    )

    sort_key_attribute = TableObjectAttribute(
        name='location_id',
        attribute_type=TableObjectAttributeType.STRING,
        description='The location ID of the source. (e.g. ISBN, URL)',
    )

    attributes = [
        TableObjectAttribute(
            name='added_on',
            attribute_type=TableObjectAttributeType.DATETIME,
            description='The date and time the source was added to the Cortex.',
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name='metadata',
            attribute_type=TableObjectAttributeType.JSON,
            description='Additional metadata about the source.',
            optional=True,
        ),

        TableObjectAttribute(
            name='publication_date',
            attribute_type=TableObjectAttributeType.DATETIME,
            description='The publication date of the source.',
            optional=True,
        ),

        TableObjectAttribute(
            name='source_id',
            attribute_type=TableObjectAttributeType.STRING,
            description='The unique identifier of the source.',
            default=lambda: str(uuid4()),
        ),
    ]


class SourcesScanDefinition(TableScanDefinition):
    def __init__(self):
        super().__init__(
            table_object_class=Source,
        )


class SourcesClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        """
        Initialize the Sources Client

        Keyword Arguments:
            app_name -- The name of the app.
            deployment_id -- The deployment ID.
        """
        super().__init__(
            app_name=app_name,
            deployment_id=deployment_id,
            default_object_class=Source,
        )

    def delete(self, source: Source) -> None:
        """
        Delete a source object from the table

        Keyword Arguments:
            source_category -- The category of the source.
            location_id -- The location ID of the source.
        """
        return self.delete_object(source)

    def get(self, source_category: Union[SourceCategory, str], location_id: str) -> Union[Source, None]:
        """
        Get a source by category and location ID

        Keyword Arguments:
            source_category -- The category of the source.
            location_id -- The location ID of the source.

        Returns:
            The source if found, otherwise None.
        """

        return self.get_object(
            partition_key_value=source_category,
            sort_key_value=location_id,
        )

    def get_by_source_id(self, source_id: str) -> Union[Source, None]:
        """
        Get a source by the unique source id

        Keyword arguments:
        source_id -- The source_id to retrieve
        """
        query_params = {
            'TableName': self.table_endpoint_name,
            'IndexName': "source_id-index",
            'KeyConditionExpression': 'SourceId = :source_id',
            'ExpressionAttributeValues': {
                ':source_id': {'S': source_id},
            },
        }

        # Execute the query
        response = self.client.query(**query_params)

        items = response.get('Items', [])

        if not items:
            return None

        return Source.from_dynamodb_item(items[0])

    def put(self, source: Source) -> None:
        """
        Put a source

        Keyword Arguments:
            source -- The source to put.

        Returns:
            The source.
        """
        return self.put_object(source)