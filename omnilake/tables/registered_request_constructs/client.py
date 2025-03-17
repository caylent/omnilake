import logging

from dataclasses import dataclass
from datetime import datetime, UTC as utc_tz
from enum import StrEnum
from typing import Dict, Optional, Union, Type

from da_vinci.core.orm.client import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
)

from da_vinci.core.immutable_object import ObjectBodySchema


__all_required_operations__ = {
    "ARCHIVE": ["LOOKUP", "PROVISION"],
    "PROCESSOR": ["PROCESS"],
    "RESPONDER": ["RESPOND"],
}


class UnsupportedOperationError(Exception):
    def __init__(self, operation: str, contruct_name: str, construct_type: str):
        super().__init__(f"Operation '{operation}' is not supported by construct '{contruct_name}' of type '{construct_type}'")


class RequestConstructType(StrEnum):
    ARCHIVE = "ARCHIVE"
    PROCESSOR = "PROCESSOR"
    RESPONDER = "RESPONDER"


@dataclass
class ArchiveConstructSchemas:
    lookup: Type[ObjectBodySchema]
    provision: Type[ObjectBodySchema]

    def to_dict(self) -> Dict:
        """
        Return the object as a dictionary
        """
        return {
            "lookup": self.lookup.to_dict(),
            "provision": self.provision.to_dict(),
        }


class RegisteredRequestConstruct(TableObject):
    table_name = "registered_request_constructs"

    description = "Stores information about registered request constructs. i.e. ARCHIVE, PROCESSOR, RESPONDER"

    partition_key_attribute = TableObjectAttribute(
        name="registered_construct_type",
        attribute_type=TableObjectAttributeType.STRING,
        description="The type of the construct (i.e. ARCHIVE, PROCESSOR, RESPONDER)",
    )

    sort_key_attribute = TableObjectAttribute(
        name="registered_type_name",
        attribute_type=TableObjectAttributeType.STRING,
        description="The registered type name of the construct",
    )

    attributes = [
        TableObjectAttribute(
            name="additional_supported_operations",
            attribute_type=TableObjectAttributeType.STRING_SET,
            optional=True,
        ),

        TableObjectAttribute(
            name="description",
            attribute_type=TableObjectAttributeType.STRING,
            description="The description of the construct",
        ),

        TableObjectAttribute(
            name="registered_on",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The time the construct was created",
            default=lambda: datetime.now(utc_tz),
        ),

        TableObjectAttribute(
            name="schemas",
            attribute_type=TableObjectAttributeType.JSON_STRING,
            description="The object schemas of the construct, i.e. lookup, provision",
            optional=True,
            default=None,
        ),
    ]

    def __init__(self, registered_construct_type: str, registered_type_name: str,
                 additional_supported_operations: Optional[list] = None, description: Optional[str] = None,
                 registered_on: Optional[Union[datetime, str]] = None,
                 schemas: Optional[Union[ArchiveConstructSchemas, Dict]] = None):
        """
        Initialize the registered request construct

        Keyword arguments:
        registered_construct_type -- The type of the construct (i.e. ARCHIVE, PROCESSOR, RESPONDER)
        registered_type_name -- The registered type name of the construct
        additional_supported_operations -- Additional supported operations for the construct
        description -- The description of the construct
        registered_on -- The time the construct was created
        schemas -- The object schemas of the construct, i.e. lookup, provision
        """
        if isinstance(registered_on, str):
            registered_on = datetime.fromisoformat(registered_on)

        if isinstance(schemas, ArchiveConstructSchemas):
            schemas = schemas.to_dict()

        super().__init__(
            registered_construct_type=registered_construct_type,
            registered_type_name=registered_type_name,
            additional_supported_operations=additional_supported_operations,
            description=description,
            registered_on=registered_on,
            schemas=schemas,
        )

    def get_object_body_schema(self, operation: str) -> Union[ObjectBodySchema, None]:
        """
        Return the object body schema for the operation

        Keyword arguments:
        operation -- The operation to get the object body schema for
        """
        operation_lwr = operation.lower()

        if operation_lwr not in self.schemas:
            logging.debug(f"Operation '{operation}' not found in schemas")

            return None

        raw_schema = self.schemas[operation_lwr]

        obj_name_components = [
            self.registered_construct_type.capitalize(),
            self.registered_type_name.capitalize(),
            operation_lwr.capitalize(),
            'Schema'
        ]

        object_name = ''.join(obj_name_components)

        logging.debug(f"Returning schema object {object_name}: {raw_schema}")

        return ObjectBodySchema.from_dict(
            object_name=object_name,
            schema_dict=raw_schema,
        )

    def get_operation_event_name(self, operation: str) -> str:
        """
        Return the event name for the operation

        Keyword arguments:
        operation -- The operation to get the event name for
        """
        operation_compare = operation.upper()

        required_operations = __all_required_operations__[self.registered_construct_type]

        additional_ops = self.additional_supported_operations or []

        if additional_ops and isinstance(additional_ops, set):
            additional_ops = list(additional_ops)

        all_operations = required_operations + additional_ops

        all_operations_normalized = [operation.upper() for operation in all_operations]

        # Check if the operation is a required/expected operation
        if operation_compare not in all_operations_normalized:
            raise UnsupportedOperationError(operation, self.registered_type_name, self.registered_construct_type)

        return f"omnilake_{self.registered_construct_type}_{self.registered_type_name}_{operation}".lower()


class RegisteredRequestConstructsClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        super().__init__(
            app_name=app_name,
            default_object_class=RegisteredRequestConstruct,
            deployment_id=deployment_id,
        )

    def delete(self, construct: RegisteredRequestConstruct) -> None:
        """
        Delete an omnilake construct

        Keyword arguments:
        omnilake_type -- The type of the construct
        registered_type_name -- The registered type name of the construct
        """
        self.delete_object(construct)

    def get(self, registered_construct_type: str, registered_type_name: str) -> Optional[RegisteredRequestConstruct]:
        """
        Get an 

        Keyword arguments:
        """
        return self.get_object(partition_key_value=registered_construct_type, sort_key_value=registered_type_name)

    def put(self, construct: RegisteredRequestConstruct) -> RegisteredRequestConstruct:
        """
        Put an omnilake construct

        Keyword arguments:
        construct -- The construct to put
        """
        return self.put_object(construct)