from datetime import datetime, UTC as utc_tz
from typing import Dict, Optional
from uuid import uuid4

from da_vinci.core.orm import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
    TableScanDefinition,
)


class SourceType(TableObject):
    table_name = 'source_types'

    description = 'Omnilake Cited Data Source Types'

    partition_key_attribute = TableObjectAttribute(
        name='source_type_name',
        attribute_type=TableObjectAttributeType.STRING,
        description='The name of the source type',
    )

    attributes = [
        TableObjectAttribute(
            name='created_on',
            attribute_type=TableObjectAttributeType.DATETIME,
            description='The date and time the source type was created',
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name='description',
            attribute_type=TableObjectAttributeType.STRING,
            description='A description of the source type',
            optional=True,
        ),

        TableObjectAttribute(
            name='required_fields',
            attribute_type=TableObjectAttributeType.STRING_LIST,
            description='The required fields for the source type',
        ),
    ]

    def generate_key(self, source_arguments: Dict, key_separator: str = '/') -> str:
        """
        Generate a unique attribute key for a source.

        Keyword Arguments:
        source_arguments -- The source arguments to use to generate the key.
        """
        key_parts = []

        for key in self.required_fields:
            if key not in source_arguments:
                raise ValueError(f'Missing required field {key}')

            key_parts.append(source_arguments[key])

        return key_separator.join(key_parts)


class SourceTypesClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        super().__init__(
            app_name=app_name,
            default_object_class=SourceType,
            deployment_id=deployment_id,
        )

    def get(self, source_type_name: str) -> Optional[SourceType]:
        """
        Get a source type by name.

        Keyword Arguments:
        source_type_name -- The name of the source type to get.
        """
        return self.get_object(partition_key_value=source_type_name)

    def put(self, source_type: SourceType) -> SourceType:
        """
        Put a source type.

        Keyword Arguments:
        source_type -- The source type to put.
        """
        return self.put_object(source_type)