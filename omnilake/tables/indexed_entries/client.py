from datetime import datetime, UTC as utc_tz
from typing import List, Optional

from da_vinci.core.orm.client import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
    TableScanDefinition,
)


class IndexedEntry(TableObject):
    table_name = "indexed_entries"

    description = "Entries indexed into archives"

    partition_key_attribute = TableObjectAttribute(
        name="archive_id",
        attribute_type=TableObjectAttributeType.STRING,
        description="The ID of the archive the entry belongs to",
    )

    sort_key_attribute = TableObjectAttribute(
        name="entry_id",
        attribute_type=TableObjectAttributeType.STRING,
        description="The ID of the entry",
    )

    attributes = [
        TableObjectAttribute(
            name="added_on",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The datetime the entry was added",
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name="effective_on",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The date and time the entry is effective on.",
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name="original_of_source",
            attribute_type=TableObjectAttributeType.STRING,
            description="The source resource name if the entry is original content of a source",
        ),

        TableObjectAttribute(
            name="tags",
            attribute_type=TableObjectAttributeType.STRING_LIST,
            description="The tags associated with the entry",
            default=[],
        ),
    ]

    def __init__(self, archive_id: str, entry_id: str, added_on: Optional[datetime] = None,
                 effective_on: Optional[datetime] = None, original_of_source: Optional[str] = None,
                 tags: Optional[List[str]] = None):
        """
        Initialize the IndexedEntry object.

        Keyword arguments:
        archive_id -- The ID of the archive
        entry_id -- The ID of the entry
        added_on -- The datetime the entry was added
        effective_on -- The date and time the entry is effective on.
        original_of_source -- The source resource name if the entry is original content of a source
        tags -- The tags associated with the entry
        """
        super().__init__(
            archive_id=archive_id,
            entry_id=entry_id,
            added_on=added_on,
            effective_on=effective_on,
            original_of_source=original_of_source,
            tags=tags,
        )

    @staticmethod
    def calculate_tag_match_percentage(object_tags: List[str], target_tags: List[str]) -> int:
        """
        Calculate the match percentage between the object's tags and the target tags.

        Keyword arguments:
        object_tags -- The list of tags to compare
        target_tags -- The list of tags to compare
        """
        matching_tags = set(object_tags) & set(target_tags)

        # Calculate the match percentage
        return len(matching_tags) / len(target_tags) * 100

    def calculate_score(self, target_tags: List[str]) -> int:
        """
        Calculate the match percentage based on the target tags.

        Keyword arguments:
        target_tags -- The target tags to calculate the score against.
        """
        tag_score = self.calculate_tag_match_percentage(
            object_tags=self.tags,
            target_tags=target_tags,
        )

        return tag_score


class IndexedEntriesScanDefinition(TableScanDefinition):
    def __init__(self):
        super().__init__(table_object_class=IndexedEntry)


class IndexedEntriesClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        super().__init__(
            app_name=app_name,
            default_object_class=IndexedEntry,
            deployment_id=deployment_id,
        )

    def delete(self, indexed_entry: IndexedEntry) -> None:
        """
        Delete an entry from the table.

        Keyword arguments:
        archive_id -- The ID of the archive
        entry_id -- The ID of the entry
        """
        return self.delete_object(table_object=indexed_entry)

    def get(self, archive_id: str, entry_id: str) -> Optional[IndexedEntry]:
        """
        Get an entry from the table.

        Keyword arguments:
        archive_id -- The ID of the archive
        entry_id -- The ID of the entry
        """
        return self.get_object(partition_key_value=archive_id, sort_key_value=entry_id)

    def put(self, entry: IndexedEntry) -> None:
        """
        Put an entry into the table.

        Keyword arguments:
        entry -- The entry to put
        """
        return self.put_object(table_object=entry)