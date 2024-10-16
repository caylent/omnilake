from datetime import datetime, UTC as utc_tz
from typing import List, Optional, Union
from uuid import uuid4

from da_vinci.core.orm import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
    TableScanDefinition,
)


class VectorStore(TableObject):
    table_name = 'vector_stores'

    description = 'Tracks all of the vector stores in the system.'

    partition_key_attribute = TableObjectAttribute(
        name='archive_id',
        attribute_type=TableObjectAttributeType.STRING,
        description='The unique identifier for the archive the vector store belongs to.',
    )

    sort_key_attribute = TableObjectAttribute(
        name='vector_store_id',
        attribute_type=TableObjectAttributeType.STRING,
        description='The unique name of the vector store.',
        default=lambda: str(uuid4())
    )

    attributes = [
        TableObjectAttribute(
            name='bucket_name',
            attribute_type=TableObjectAttributeType.STRING,
            description='The S3 bucket name where the vector store content is stored.',
        ),

        TableObjectAttribute(
            name='created_on',
            attribute_type=TableObjectAttributeType.DATETIME,
            description='The date and time the vector store was created.',
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name='last_rebalanced',
            attribute_type=TableObjectAttributeType.DATETIME,
            description='The date and time the vector store was last rebalanced.',
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name='total_entries',
            attribute_type=TableObjectAttributeType.NUMBER,
            description='The total number of entries in the vector store.',
            default=0,
        ),

        TableObjectAttribute(
            name='total_entries_last_calculated',
            attribute_type=TableObjectAttributeType.DATETIME,
            description='The date and time the total entries was last calculated.',
            default=lambda: datetime.now(utc_tz),
        ),
    ]

    @property
    def vector_store_name(self) -> str:
        """
        Returns the full name used for vector store tables. 
        """
        return f"{self.archive_id}-{self.vector_store_id}"


class VectorStoresScanDefinition(TableScanDefinition):
    def __init__(self):
        super().__init__(table_object_class=VectorStore)


class VectorStoresClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        super().__init__(
            app_name=app_name,
            deployment_id=deployment_id,
            default_object_class=VectorStore
        )

    def all(self) -> List[VectorStore]:
        """
        Get all vector stores in the table.
        """
        return self._all_objects()

    def delete(self, vector_store: VectorStore) -> None:
        """
        Delete a vector store from the table.

        Keyword Arguments:
        vector_store -- The vector store object to delete.
        """
        self.delete_object(vector_store)

    def get(self, archive_id: str, vector_store_id: str) -> Union[VectorStore, None]:
        """
        Get a vector store by its unique name.

        Keyword Arguments:
        archive_id -- The unique identifier for the archive the vector store belongs to.
        vector_store_id -- The unique identifier of the vector store to retrieve. If not provided, all vector stores for the archive will be returned.
        """
        return self.get_object(
            partition_key_value=archive_id,
            sort_key_value=vector_store_id
        )

    def get_by_archive(self, archive_id: str) -> List[VectorStore]:
        """
        Get all vector stores for an archive.

        Keyword Arguments:
        archive_id -- The unique identifier for the archive to retrieve vector stores for.
        """
        params = {
            'KeyConditionExpression': '#pk = :pv',
            'ExpressionAttributeValues': {':pv': {'S': archive_id}},
            'ExpressionAttributeNames': {'#pk': 'ArchiveId'},
        }

        result = []

        for page in self.paginated(call='query', parameters=params):
            for vector_store in page:
                result.append(vector_store)

        return result

    def put(self, vector_store: VectorStore) -> None:
        """
        Put a vector store into the table.

        Keyword Arguments:
        vector_store -- The vector store to put into the table.
        """
        return self.put_object(vector_store)