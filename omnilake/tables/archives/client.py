from datetime import datetime, UTC as utc_tz
from enum import StrEnum
from typing import Optional, Union

from da_vinci.core.orm import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
)

class ArchiveStatus(StrEnum):
    ACTIVE = "ACTIVE"
    CREATING = "CREATING"
    DELETING = "DELETING"
    MAINTENANCE = "MAINTENANCE"


class ArchiveVisibility(StrEnum):
    PUBLIC = "PUBLIC"
    SYSTEM = "SYSTEM"


class Archive(TableObject):
    table_name = "archives"

    description = "Stores information about an archive"

    partition_key_attribute = TableObjectAttribute(
        name="archive_id",
        attribute_type=TableObjectAttributeType.STRING,
        description="The ID of the archive",
    )

    attributes = [
        TableObjectAttribute(
            name="created_on",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The time the archive was created",
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name="description",
            attribute_type=TableObjectAttributeType.STRING,
            description="The description of the archive",
        ),

        TableObjectAttribute(
            name="status",
            attribute_type=TableObjectAttributeType.STRING,
            description="The status of the archive",
            default=ArchiveStatus.ACTIVE, # ACTIVE, CREATING, DELETING or MAINTENANCE
        ),

        TableObjectAttribute(
            name="status_context_job_ids",
            attribute_type=TableObjectAttributeType.STRING_LIST,
            description="The job IDs associated with the status of the archive",
            default=[],
            optional=True,
        ),

        # For future use in the event we want to use other storage types
        TableObjectAttribute(
            name="storage_type",
            attribute_type=TableObjectAttributeType.STRING,
            description="The storage type of the archive",
            default="VECTOR_STORAGE",
        ),

        TableObjectAttribute(
            name="updated_on",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The time the archive was last updated",
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name="visibility",
            attribute_type=TableObjectAttributeType.STRING,
            description="The visibility of the archive",
            default=ArchiveVisibility.PUBLIC,
        )
    ]


class ArchivesClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        super().__init__(
            app_name=app_name,
            default_object_class=Archive,
            deployment_id=deployment_id,
        )

    def delete(self, archive: str):
        """
        Delete an archive

        Keyword arguments:
        archive -- The archive to delete
        """
        self.delete_object(table_object=archive)

    def get(self, archive_id: str) -> Union[Archive, None]:
        """
        Get an archive by ID

        Keyword arguments:
        archive_id -- The ID of the archive
        """
        return self.get_object(partition_key_value=archive_id)

    def put(self, archive: Archive) -> None:
        """
        Put an archive

        Keyword arguments:
        archive -- The archive to put
        """
        return self.put_object(table_object=archive)