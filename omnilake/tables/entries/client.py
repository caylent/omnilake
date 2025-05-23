from datetime import datetime, UTC as utc_tz
from hashlib import sha256
from typing import List, Optional, Union
from uuid import uuid4

from da_vinci.core.orm import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
    TableScanDefinition,
)


class Entry(TableObject):
    table_name = "entries"
    description = "Tracks all of the data entries in the system."

    partition_key_attribute = TableObjectAttribute(
        name="entry_id",
        attribute_type=TableObjectAttributeType.STRING,
        description="The unique identifier for the entry.",
        default=lambda: str(uuid4()),
    )

    attributes = [
        TableObjectAttribute(
            name="char_count",
            attribute_type=TableObjectAttributeType.NUMBER,
            description="The number of characters in the entry.",
            optional=True,
            default=0,
        ),

        TableObjectAttribute(
            name="content_hash",
            attribute_type=TableObjectAttributeType.STRING,
            description="The hash of the content of the entry.",
            optional=True,
        ),

        TableObjectAttribute(
            name="created_on",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The date and time the entry was created.",
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name="effective_on",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The date and time the entry is effective on.",
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name="last_accessed_on",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The date and time the entry was last accessed.",
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name="original_of_source",
            attribute_type=TableObjectAttributeType.STRING,
            description="The source resource name the entry is an original of.",
            default=None,
            optional=True,
        ),

        TableObjectAttribute(
            name="sources",
            attribute_type=TableObjectAttributeType.STRING_SET,
            description="The source resource names for the entry.",
        ),
    ]

    def __init__(self, entry_id: Optional[str] = None, char_count: Optional[int] = None,
                 content_hash: Optional[str] = None, created_on: Optional[datetime] = None,
                 effective_on: Optional[datetime] = None, last_accessed_on: Optional[datetime] = None,
                 original_of_source: Optional[str] = None, sources: Optional[List[str]] = None):
        """
        Initialize an entry object.

        Keyword arguments:
        entry_id -- The unique identifier for the entry. Will auto-generate a uuid if none is provided.
        char_count -- The number of characters in the entry.
        content_hash -- The hash of the content of the entry.
        effective_on -- The date and time the entry is effective on.
        last_accessed_on -- The date and time the entry was last accessed.
        original_of_source -- The source resource name the entry represents original content for.
        sources -- The source resource names for the entry.
        """

        super().__init__(
            entry_id=entry_id,
            char_count=char_count,
            content_hash=content_hash,
            created_on=created_on,
            effective_on=effective_on,
            last_accessed_on=last_accessed_on,
            original_of_source=original_of_source,
            sources=sources,
        )

    @staticmethod
    def calculate_hash(content: str) -> str:
        """
        Generate a hash of the content of the entry.

        Keyword arguments:
        content -- The content of the entry.
        """
        content_hash = sha256(content.encode('utf-8'))

        return content_hash.hexdigest()


class EntriesScanDefinition(TableScanDefinition):
    def __init__(self):
        super().__init__(table_object_class=Entry)


class EntriesClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        super().__init__(
            app_name=app_name,
            deployment_id=deployment_id,
            default_object_class=Entry,
        )

    def delete(self, entry: Entry) -> None:
        """
        Delete an entry from the system.
        """
        return self.delete_object(entry)

    def get(self, entry_id: str) -> Union[Entry, None]:
        """
        Get an entry by its unique identifier.

        Keyword arguments:
        entry_id -- The unique identifier of the entry.
        """
        return self.get_object(partition_key_value=entry_id)

    def put(self, entry: Entry) -> None:
        """
        Put an entry into the system.

        Keyword arguments:
        entry -- The entry to put into the system.
        """
        return self.put_object(entry)