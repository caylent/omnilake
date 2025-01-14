import json

from datetime import datetime
from enum import StrEnum
from typing import Any, List, Optional, Union, Type

from da_vinci.core.client_base import RESTClientBase, RESTClientResponse


__version__ = '2025.1.1'


class RequestAttributeError(Exception):
    def __init__(self, attribute_name: str, error: str = "Missing required attribute"):
        """
        A custom exception for missing required request attributes.

        Keyword arguments:
        attribute_name -- The name of the attribute that is missing
        """
        self.attribute_name = attribute_name

        super().__init__(f"{error}: {attribute_name}")


class RequestAttributeType(StrEnum):
    BOOLEAN = 'BOOLEAN'
    DATETIME = 'DATETIME'
    FLOAT = 'FLOAT'
    LIST = 'LIST'
    INTEGER = 'INTEGER'
    OBJECT = 'OBJECT'
    OBJECT_LIST = 'OBJECT_LIST'
    STRING = 'STRING'


class RequestBodyAttribute:
    def __init__(self, name: str, attribute_type: Union[RequestAttributeType, Type] = RequestAttributeType.STRING,
                 attribute_subtype: Optional[Union[RequestAttributeType, Type]] = None, default: Optional[str] = None,
                 immutable_default: Optional[str] = None, optional: Optional[bool] = False,
                 supported_request_body_types: Optional[Union[Type["RequestBody"], List[Type["RequestBody"]]]] = None):
        """
        Initialize the request body attribute.

        Keyword arguments:
        name -- The name of the attribute
        attribute_type -- The type of the attribute
        attribute_subtype -- The subtype of the attribute
        default -- The default value of the attribute
        immutable_default -- The immutable default value of the attribute
        optional -- Whether the attribute is optional
        supported_request_body_types -- The supported request body types for the attribute
        """
        self.name = name

        self.attribute_type = attribute_type

        self.attribute_subtype = attribute_subtype

        self.default = default

        self.immutable_default = immutable_default

        self.optional = optional

        if isinstance(supported_request_body_types, str):
            supported_request_body_types = [supported_request_body_types]

        self.supported_request_body_types = supported_request_body_types

    def validate_type(self, value: Any):
        """
        Validate the type of a value. Override this method to add custom validation logic.

        Keyword arguments:
        value -- The value to validate
        """
        if self.attribute_type == RequestAttributeType.BOOLEAN:
            return isinstance(value, bool)

        # Supports both datetime objects and strings in the format 'YYYY-MM-DD HH:MM:SS'
        elif self.attribute_type == RequestAttributeType.DATETIME:
            if isinstance(value, datetime):
                return True
            
            try:
                datetime.fromisoformat(value)

                return True
            
            except ValueError:
                return False

        elif self.attribute_type == RequestAttributeType.FLOAT:
            return isinstance(value, float)

        elif self.attribute_type == RequestAttributeType.LIST:
            return isinstance(value, list)
        
        elif self.attribute_type == RequestAttributeType.INTEGER:
            return isinstance(value, int)

        elif self.attribute_type == RequestAttributeType.OBJECT:
            if isinstance(value, RequestBody):
                if self.supported_request_body_types is not None:
                    return isinstance(value, tuple(self.supported_request_body_types))

                return True

            return isinstance(value, dict)

        elif self.attribute_type == RequestAttributeType.OBJECT_LIST:
            if not isinstance(value, list):
                return False
            
            for item in value:
                if isinstance(item, dict):
                    return True
                
                if isinstance(item, RequestBody):
                    if self.supported_request_body_types:
                        return isinstance(item, self.supported_request_body_types)

                    return True

        elif self.attribute_type == RequestAttributeType.STRING:
            return isinstance(value, str)

        return False


class RequestBody:
    """
    Represents a request body for a request to the omnilake API.

    Keyword arguments:
    attributes -- The attributes of the request
    path -- The path of the request
    """
    attribute_definitions: List[RequestBodyAttribute]
    path: str = None

    def __init__(self, **kwargs):
        """
        Initialize the superclass.

        Keyword arguments:
        kwargs -- The attributes of the request
        """
        self.attributes = {}

        for attr in self.attribute_definitions:
            attr_val = kwargs.get(attr.name, attr.default)

            if attr.immutable_default:
                # If the attribute is immutable, it cannot be set
                if attr_val:
                    raise RequestAttributeError(attribute_name=attr.name, error="Immutable attribute cannot be set")

                attr_val = attr.immutable_default

            elif not attr_val:
                if attr.optional:
                    attr_val = attr.default

                else:
                    raise RequestAttributeError(attribute_name=attr.name)

            elif attr_val:
                if not attr.validate_type(attr_val):
                    raise RequestAttributeError(attribute_name=attr.name, error="Invalid type for attribute")

            if attr.attribute_type == RequestAttributeType.DATETIME and isinstance(attr_val, datetime):
                # Convert datetime objects to strings to ensure they are serialized correctly
                attr_val = attr_val.isoformat()

            self.attributes[attr.name] = attr_val

    def to_dict(self):
        """
        Return the object as a dictionary. Supports nested RequestBody objects.
        """
        prepped_attributes = {}

        for key, value in self.attributes.items():
            if isinstance(value, RequestBody):
                prepped_attributes[key] = value.to_dict()

        return self.attributes


class OmniClientJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()

        elif isinstance(obj, RequestBody):
            return obj.to_dict()

        return json.JSONEncoder.default(self, obj)


class OmniLake(RESTClientBase):
    def __init__(self, app_name: str = 'omnilake', deployment_id: str = 'dev'):
        super().__init__(
            resource_name='omnilake-private-api',
            app_name=app_name,
            deployment_id=deployment_id
        )

    def request(self, request: RequestBody) -> RESTClientResponse:
        """
        Make a request to omnilake.

        Keyword arguments:
        request -- The request to make to omnilake.
        """
        if not request.path:
            raise ValueError('Request object does not have a defined path')

        request_body = json.dumps(request, cls=OmniClientJSONEncoder)

        return self.post(
            body=json.loads(request_body),
            path=request.path,
        )