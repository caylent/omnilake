from datetime import datetime, UTC as utc_tz
from typing import List, Optional, Union

from da_vinci.core.orm import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
    TableScanDefinition,
)


class VectorStoreChunk(TableObject):
    table_name = "vector_store_chunks"

    description = "A chunk of an entry stored in the vector store"

    partition_key_attribute = TableObjectAttribute(
        name="archive_id",
        attribute_type=TableObjectAttributeType.STRING,
        description="The ID of the archive that this chunk belongs to",
    )

    sort_key_attribute = TableObjectAttribute(
        name="chunk_id",
        attribute_type=TableObjectAttributeType.STRING,
        description="The ID of this chunk",
    )

    attributes = [
        TableObjectAttribute(
            name="created_at",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The timestamp of when this chunk was created",
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name="entry_id",
            attribute_type=TableObjectAttributeType.STRING,
            description="The ID of the entry that this chunk belongs to",
        ),

        TableObjectAttribute(
            name="vector_store_id",
            attribute_type=TableObjectAttributeType.STRING,
            description="The vector store ID that this chunk belongs to",
        ),
    ]


class VectorStoreChunksScanDefinition(TableScanDefinition):
    def __init__(self):
        super().__init__(table_object_class=VectorStoreChunk)


class VectorStoreChunksClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        super().__init__(
            app_name=app_name,
            default_object_class=VectorStoreChunk,
            deployment_id=deployment_id
        )

    def delete(self, chunk: VectorStoreChunk) -> None:
        """
        Delete a chunk of an entry stored in the vector store

        Keyword Arguments:
        chunk -- The chunk to delete
        """
        self.delete_object(chunk)

    def get(self, archive_id: str, chunk_id: str) -> Union[VectorStoreChunk, None]:
        """
        Get a chunk of an entry stored in the vector store

        Keyword Arguments:
        archive_id -- The ID of the archive that this chunk belongs to
        chunk_id -- The ID of this chunk
        """
        return self.get_object(partition_key_value=archive_id, sort_key_value=chunk_id)

    def get_by_archive(self, archive_id: str) -> List[VectorStoreChunk]:
        """
        Get all chunks that belong to an archive

        Keyword Arguments:
        archive_id -- The ID of the archive that this chunk belongs to
        """
        params = {
            "KeyConditionExpression": "ArchiveId = :archive_id",
            "ExpressionAttributeValues": {":archive_id": {"S": archive_id}},
        }

        chunks = []

        for page in self.paginated(call="query", parameters=params):
            chunks.extend(page)

        return chunks

    def get_chunks_by_archive_and_entry(self, archive_id: str, entry_id: str) -> List[VectorStoreChunk]:
        """
        Get all chunks that belong to an archive and entry

        Keyword Arguments:
        archive_id -- The ID of the archive that this chunk belongs to
        entry_id -- The ID of the entry that this chunk belongs to
        """
        params = {
            "KeyConditionExpression": "ArchiveId = :archive_id AND EntryId = :entry_id",
            "ExpressionAttributeValues":{
                ":archive_id": {"S": archive_id},
                ":entry_id": {"S": entry_id}
            },
            "IndexName": "archive_entry-index"
        }

        resulting_chunks = []

        for page in self.paginated(call="query", parameters=params):
            resulting_chunks.extend(page)

        return resulting_chunks

    def get_by_vector_store_id(self, archive_id: str, vector_store_id: str) -> List[VectorStoreChunk]:
        """
        Get all chunks that belong to a vector store

        Keyword Arguments:
        archive_id -- The ID of the archive that this chunk belongs to
        vector_store_id -- The ID of the vector store
        """
        query_params = {
            "KeyConditionExpression": "ArchiveId = :archive_id and VectorStoreId = :vector_store_id",
            "ExpressionAttributeValues": {
                ":vector_store_id": {"S": vector_store_id},
                ":archive_id": {"S": archive_id},
            },
            "IndexName": "vector_id-index",
        }

        chunks = []

        for page in self.paginated(call="query", parameters=query_params):
            chunks.extend(page)

        return chunks

    def put(self, chunk: VectorStoreChunk) -> None:
        """
        Put a chunk of an entry stored in the vector store

        Keyword Arguments:
        chunk -- The chunk to put
        """
        self.put_object(chunk)