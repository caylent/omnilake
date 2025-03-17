from datetime import datetime, UTC as utc_tz
from typing import List, Optional, Union
from uuid import uuid4

from da_vinci.core.orm.client import (
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

        TableObjectAttribute(
            name='vector_store_id',
            attribute_type=TableObjectAttributeType.STRING,
            description='The unique id of the vector store',
            default=lambda: str(uuid4())
        ),
    ]

    def __init__(self, archive_id: str, bucket_name: str, created_on: Optional[datetime] = None,
                 total_entries: Optional[int] = 0, total_entries_last_calculated: Optional[datetime] = None,
                 vector_store_id: Optional[str] = None):
        """
        Initialize a new vector store object.

        Keyword Arguments:
        archive_id -- The unique identifier for the archive the vector store belongs to.
        bucket_name -- The S3 bucket name where the vector store content is stored.
        created_on -- The date and time the vector store was created.
        total_entries -- The total number of entries in the vector store.
        total_entries_last_calculated -- The date and time the total entries was last calculated.
        vector_store_id -- The unique name of the vector store.
        """
        super().__init__(
            archive_id=archive_id,
            bucket_name=bucket_name,
            created_on=created_on,
            total_entries=total_entries,
            total_entries_last_calculated=total_entries_last_calculated,
            vector_store_id=vector_store_id,
        )


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

    def get(self, archive_id: str) -> Union[VectorStore, None]:
        """
        Get a vector store by its unique name.

        Keyword Arguments:
        archive_id -- The unique identifier for the archive the vector store belongs to.
        """
        return self.get_object(partition_key_value=archive_id)

    def put(self, vector_store: VectorStore) -> None:
        """
        Put a vector store into the table.

        Keyword Arguments:
        vector_store -- The vector store to put into the table.
        """
        return self.put_object(vector_store)