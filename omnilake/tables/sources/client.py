from datetime import datetime, UTC as utc_tz
from typing import Optional, Union
from uuid import uuid4

from da_vinci.core.orm import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
    TableScanDefinition,
)


class Source(TableObject):
    table_name = 'sources'

    description = 'Omnilake Cited Data Sources'

    partition_key_attribute = TableObjectAttribute(
        name='source_type',
        attribute_type=TableObjectAttributeType.STRING,
        description='The category of the source. (e.g. ARTICLE, BOOK, INTERNAL_KNOWLEDGE, MEDIA, PERSONAL_COMMUNICATION, REPORT, WEBSITE)',
    )

    sort_key_attribute = TableObjectAttribute(
        name='source_id',
        attribute_type=TableObjectAttributeType.STRING,
        description='The generated unique identifier of the source.',
        default=lambda: str(uuid4()),
    )

    attributes = [
        TableObjectAttribute(
            name='added_on',
            attribute_type=TableObjectAttributeType.DATETIME,
            description='The date and time the source was added to the Omnilake.',
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name='attribute_key',
            attribute_type=TableObjectAttributeType.STRING,
            description='The unique key of the source, created by combining the source attributes',
            default=lambda: str(uuid4()),
        ),

        TableObjectAttribute(
            name='latest_content_entry_id',
            attribute_type=TableObjectAttributeType.STRING,
            description='The latest content entry ID of the source.',
            optional=True,
        ),

        TableObjectAttribute(
            name='source_arguments',
            attribute_type=TableObjectAttributeType.JSON,
            description='Information about the source.',
            optional=True,
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
            source_type -- The category of the source.
            source_id -- The location ID of the source.
        """
        return self.delete_object(source)

    def get(self, source_type: str, source_id: str) -> Union[Source, None]:
        """
        Get a source by category and location ID

        Keyword Arguments:
            source_type -- The category of the source.
            source_id -- The location ID of the source.

        Returns:
            The source if found, otherwise None.
        """

        return self.get_object(
            partition_key_value=source_type,
            sort_key_value=source_id,
        )

    def get_by_attribute_key(self, attribute_key: str) -> Union[Source, None]:
        """
        Get a source by attribute key

        Keyword Arguments:
            attribute_key -- The attribute key of the source.

        Returns:
            The source if found, otherwise None.
        """
        params = {
            "KeyConditionExpression": "AttributeKey = :attr_key",
            "ExpressionAttributeValues":{
                ":attr_key": {"S": attribute_key},
            },
            "IndexName": "attribute-key-index"
        }

        results = []

        for page in self.paginated(call="query", parameters=params):
            results.extend(page)

        if len(results) == 0:
            return None

        return results[0]

    def put(self, source: Source) -> None:
        """
        Put a source

        Keyword Arguments:
            source -- The source to put.

        Returns:
            The source.
        """
        return self.put_object(source)