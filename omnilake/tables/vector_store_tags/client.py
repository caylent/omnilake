from datetime import datetime, UTC as utc_tz
from typing import List, Optional, Union

from da_vinci.core.orm import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
)


class VectorStoreTag(TableObject):
    table_name = 'vector_store_tags'

    description = 'Tracks all of the tags associated with vector stores in the system.'

    partition_key_attribute = TableObjectAttribute(
        name='archive_id',
        attribute_type=TableObjectAttributeType.STRING,
        description='The unique identifier for the archive the vector store belongs to.',
    )

    sort_key_attribute = TableObjectAttribute(
        name='tag',
        attribute_type=TableObjectAttributeType.STRING,
        description='The unique name of the tag.',
    )

    attributes = [
        TableObjectAttribute(
            name='created_on',
            attribute_type=TableObjectAttributeType.DATETIME,
            description='The date and time the tag was created.',
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name='vector_store_entries_count',
            attribute_type=TableObjectAttributeType.JSON,
            description='The number of vector store entries associated with the tag.',
            default={},
        ),

        TableObjectAttribute(
            name='vector_store_ids',
            attribute_type=TableObjectAttributeType.STRING_SET,
            description='A list of vector store ids associated with the tag.',
            optional=True,
        )
    ]


class VectorStoreTagsClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        """
        Initializes the VectorStoreTagsClient.

        Keyword arguments:
        app_name -- The name of the application. (default None)
        deployment_id -- The deployment id. (default None)
        """
        super().__init__(app_name=app_name, default_object_class=VectorStoreTag, deployment_id=deployment_id)

    def add_vector_store_to_tags(self, archive_id: str, vector_store_id: str, tags: List[str]) -> None:
        """
        Adds a vector store to the specified tags. If the tag does not exist, it will be created.

        Keyword arguments:
        archive_id -- The archive id.
        vector_store_id -- The vector store id.
        tags -- The list of tags.
        """
        for tag_name in tags:
            retrieved_tag = self.get(archive_id=archive_id, tag=tag_name)

            if retrieved_tag is None:
                retrieved_tag = VectorStoreTag(
                    archive_id=archive_id,
                    tag=tag_name,
                    vector_store_entries_count={vector_store_id: 1},
                    vector_store_ids=set([vector_store_id])
                )

            else:
                if vector_store_id not in retrieved_tag.vector_store_ids:
                    retrieved_tag.vector_store_ids.add(vector_store_id)

                entry_count = retrieved_tag.vector_store_entries_count.get(vector_store_id, 0)

                retrieved_tag.vector_store_entries_count[vector_store_id] = entry_count + 1

            self.put(retrieved_tag)

    def get(self, archive_id: str, tag: str) -> Union[VectorStoreTag, None]:
        """
        Retrieves a VectorStoreTag object from the table.

        Keyword arguments:
        archive_id -- The archive id.
        tag -- The tag.
        """
        return self.get_object(archive_id, tag)

    def get_all_matching_vector_stores(self, archive_id: str, tags: List[str]) -> List[str]:
        """
        Returns a list of unique vector store ids that match the specified tags using BatchGetItem.

        Keyword arguments:
        archive_id -- The archive id.
        tags -- The list of tags.
        """
        matching_vector_stores = set()  # Use a set to automatically handle duplicates

        # Prepare the request for BatchGetItem
        request_items = {
            self.table_endpoint_name: {
                'Keys': [
                    {'ArchiveId': {'S': archive_id}, 'Tag': {'S': tag}} for tag in tags
                ],
                'ProjectionExpression': 'VectorStoreIds'
            }
        }

        # Call BatchGetItem
        response = self.client.batch_get_item(RequestItems=request_items)

        # Process the response
        for item in response['Responses'].get(self.table_endpoint_name, []):
            vector_store_ids = item.get('VectorStoreIds', {}).get('L', [])
        
            # Since each 'L' list entry will contain dicts with 'S' values, we extract the actual strings
            vector_store_ids = [vs_id.get('S') for vs_id in vector_store_ids if 'S' in vs_id]

            matching_vector_stores.update(vector_store_ids)

        # Handle unprocessed keys (if any)
        while 'UnprocessedKeys' in response and response['UnprocessedKeys']:
            unprocessed_keys = response['UnprocessedKeys']

            response = self.client.batch_get_item(RequestItems=unprocessed_keys)

            for item in response['Responses'].get(self.table_endpoint_name, []):
                vector_store_ids = item.get('VectorStoreIds', {}).get('L', [])
                
                vector_store_ids = [vs_id.get('S') for vs_id in vector_store_ids if 'S' in vs_id]

                matching_vector_stores.update(vector_store_ids)

        return list(matching_vector_stores)

    def get_tags_for_vector_store(self, archive_id: str, vector_store_id: str) -> List[str]:
        """
        Returns a list of tags associated with the given archive_id and vector_store_id.
        Keyword arguments:
        archive_id -- The archive id.
        vector_store_id -- The vector store id.
        """
        response = self.client.query(
            TableName=self.table_endpoint_name,
            KeyConditionExpression='ArchiveId = :aid',
            FilterExpression='contains(VectorStoreIds, :vid)',
            ExpressionAttributeValues={
                ':aid': {'S': archive_id},
                ':vid': {'S': vector_store_id}
            },
            ProjectionExpression='Tag'
        )

        return [item['Tag']['S'] for item in response.get('Items', [])]

    def get_top_n_percent_tags(self, archive_id: str, vector_store_id: str, percentage: int) -> List[VectorStoreTag]:
        """
        Finds the top n% of tags with the highest concentration (entry count) for a given vector store.

        WARNING: This method will consume a significant amount of resources, it is recommended to increase the timeout and memory
        for any Lambda function that calls this method.

        Keyword arguments:
        archive_id -- The archive id.
        vector_store_id -- The vector store id.
        percentage -- The percentage of tags to return (e.g., 10 for top 10%).
        
        Returns a list of dictionaries with the top tags and their respective counts.
        """
        # Prepare the query parameters for paginated call
        parameters = {
            'KeyConditionExpression': 'ArchiveId = :aid',
            'FilterExpression': 'contains(VectorStoreIds, :vid)',
            'ExpressionAttributeValues': {
                ':vid': {'S': vector_store_id},
                ':aid': {'S': archive_id}
            },
        }

        tags_w_count = []

        # Paginate through the results
        for items in self.paginated(call='query', parameters=parameters):
            # Process each page of items
            for item in items:
                # Get the entry count for the vector store in the current tag
                entry_count = item.vector_store_entries_count.get(vector_store_id, 0)

                tags_w_count.append({"tag": item, "entry_count": entry_count})

        # Sort the tags by entry count in descending order
        tags_w_count.sort(key=lambda x: x['entry_count'], reverse=True)

        # Calculate how many tags to return based on the percentage
        top_n_count = max(1, int(len(tags_w_count) * (percentage * 0.01)))

        # Return the top n% of tags
        return [itm["tag"] for itm in tags_w_count[:top_n_count]]

    def remove_vector_store_from_tags(self, archive_id: str, vector_store_id: str, tags: List[str], force: Optional[bool] = False) -> None:
        """
        Removes a vector store from the specified tags.

        Keyword arguments:
        archive_id -- The archive id.
        vector_store_id -- The vector store id.
        tags -- The list of tags.
        force -- If True, the vector store will be removed from the tags even if the count is greater than 1.
        """
        for tag_name in tags:
            # Remove the vector_store_id from the set

            retrieved_tag = self.get(archive_id, tag_name)

            if retrieved_tag is None:
                continue

            if vector_store_id not in retrieved_tag.vector_store_ids:
                continue

            entry_count = retrieved_tag.vector_store_entries_count.get(vector_store_id, 0)

            if entry_count > 1 and not force:
                # If there are more than one entries associated with the vector store, decrement the count
                retrieved_tag.vector_store_entries_count[vector_store_id] = entry_count - 1

            else:
                # If there is only one entry associated with the vector store, remove the vector store id
                retrieved_tag.vector_store_entries_count.pop(vector_store_id)

                retrieved_tag.vector_store_ids.remove(vector_store_id)

            self.put(retrieved_tag)

    def put(self, tag: VectorStoreTag) -> None:
        """
        Puts a VectorStoreTag object into the table.

        Keyword arguments:
        tag -- The VectorStoreTag object.
        """
        self.put_object(tag)