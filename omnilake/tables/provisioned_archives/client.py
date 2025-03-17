from datetime import datetime, UTC as utc_tz
from enum import StrEnum
from typing import Optional, Union

from da_vinci.core.orm.client import (
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


class Archive(TableObject):
    table_name = "provisioned_archives"

    description = "Stores information about an archive"

    partition_key_attribute = TableObjectAttribute(
        name="archive_id",
        attribute_type=TableObjectAttributeType.STRING,
        description="The ID of the archive",
    )

    attributes = [
        TableObjectAttribute(
            name="archive_type",
            attribute_type=TableObjectAttributeType.STRING,
            description="The ID of the archive",
        ),

        TableObjectAttribute(
            name="configuration",
            attribute_type=TableObjectAttributeType.JSON_STRING,
            description="The configuration of the archive",
        ),

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

        TableObjectAttribute(
            name="updated_on",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The time the archive was last updated",
            default=lambda: datetime.now(utc_tz),
        ),
    ]

    def __init__(self, archive_id: str, archive_type: str, configuration: dict, description: str,
                 created_on: Optional[datetime] = None, status: Optional[ArchiveStatus] = None,
                 status_context_job_ids: Optional[list] = None, updated_on: Optional[datetime] = None):
        """
        Initialize an Archive TableObject

        Keyword arguments:
        archive_id -- The ID of the archive
        archive_type -- The type of the archive
        configuration -- The configuration of the archive
        description -- The description of the archive
        created_on -- The time the archive was created
        status -- The status of the archive
        status_context_job_ids -- The job IDs associated with the status of the archive
        updated_on -- The time the archive was last updated
        """
        super().__init__(
            archive_id=archive_id,
            archive_type=archive_type,
            configuration=configuration,
            description=description,
            created_on=created_on,
            status=status,
            status_context_job_ids=status_context_job_ids,
            updated_on=updated_on,
        )


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