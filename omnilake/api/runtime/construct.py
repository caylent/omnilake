'''
Mini API Constructor Library

Makes it easy to create this monster API while making the code a bit more maintainable.
'''
import logging

from datetime import datetime
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Dict, List, Optional, Union, Type


class InvalidPathError(ValueError):
    def __init__(self, path: str):
        super().__init__(f"Path {path} not found in route map.")


class RequestAttributeError(Exception):
    def __init__(self, attribute_name: str, error: str = "Missing required attribute"):
        """
        A custom exception for missing required request attributes.

        Keyword arguments:
        attribute_name -- The name of the attribute that is missing
        """
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


class RequestAttribute:
    def __init__(self, name: str, attribute_type: Union[RequestAttributeType, Type] = RequestAttributeType.STRING,
                 default: Optional[str] = None, immutable_default: Optional[str] = None, optional: Optional[bool] = False,
                 supported_request_body_types: Optional[Union[Type["RequestBody"], List[Type["RequestBody"]]]] = None):
        """
        Initialize the request body attribute.

        Keyword arguments:
        name -- The name of the attribute
        attribute_type -- The type of the attribute
        default -- The default value of the attribute
        immutable_default -- The immutable default value of the attribute
        optional -- Whether the attribute is optional
        supported_request_body_types -- The supported request body types for the attribute
        """
        self.name = name

        self.attribute_type = attribute_type

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
            return isinstance(value, dict) or isinstance(value, RequestBody)

        elif self.attribute_type == RequestAttributeType.OBJECT_LIST:
            if not isinstance(value, list):
                return False
            
            for item in value:
                return isinstance(item, dict) or isinstance(item, RequestBody)

        elif self.attribute_type == RequestAttributeType.STRING:
            return isinstance(value, str)

        return False


class RequestBody:
    """
    Represents a request body

    Keyword arguments:
    attributes -- The attributes of the request
    """
    attribute_definitions: List[RequestAttribute]

    def __init__(self, attributes: Dict):
        """
        Initialize the superclass.

        Keyword arguments:
        attributes -- The attributes of the request
        """
        self.loaded_schema = {attr.name: attr for attr in self.attribute_definitions}

        self.attributes = {}

        for attr in self.loaded_schema.values():
            attr_val = attributes.get(attr.name, attr.default)

            if attr.immutable_default:
                # If the attribute is immutable, it cannot be set
                if attr_val != attr.immutable_default:
                    raise RequestAttributeError(attribute_name=attr.name, error="Immutable attribute cannot be set")

                attr_val = attr.immutable_default

            if attr_val is None and attr.optional:
                    attr_val = attr.default

            else:
                raise RequestAttributeError(attribute_name=attr.name)

            if attr_val is not None and not attr.validate_type(attr_val):
                raise RequestAttributeError(attribute_name=attr.name, error="Invalid type for attribute")

            if attr.attribute_type == RequestAttributeType.DATETIME and not isinstance(attr_val, str):
                # Convert datetime objects to strings to ensure they are serialized correctly
                attr_val = attr_val.isoformat()

            self.attributes[attr.name] = attr_val

    def get(self, attribute_name: str, convert_datetime: bool = True):
        """
        Retrieve an attribute by name. This method will convert datetime strings into objects.

        Keyword arguments:
        attribute_name -- The name of the attribute
        """
        if attribute_name not in self.loaded_schema:
            raise RequestAttributeError(attribute_name=attribute_name, error="Attribute not defined")

        attr_is_datetime = self.loaded_schema[attribute_name].attribute_type == RequestAttributeType.DATETIME

        attr_value = self.attributes.get(attribute_name)

        if attr_value and attr_is_datetime and convert_datetime:
            return datetime.fromisoformat(attr_value)

        return attr_value

    def to_dict(self):
        """
        Return the object as a dictionary. Supports nested RequestBody objects.
        """
        prepped_attributes = {}

        for key, value in self.attributes.items():
            if isinstance(value, RequestBody):
                prepped_attributes[key] = value.to_dict()

        return self.attributes


@dataclass
class Route:
    path: str
    method_name: str
    request_body: Type[RequestBody] = None


class ChildAPI:
    routes: List[Route] = []

    def  __init__(self):
        self._route_map = {route.path: route for route in self.routes}

    def execute_path(self, path: str, **kwargs):
        """
        Execute a path

        Keyword arguments:
        path -- The path
        """
        if path not in self._route_map:
            raise InvalidPathError(path)

        route_value = self._route_map[path]

        if route_value.request_body:
            return getattr(self, route_value.method_name)(route_value.request_body(kwargs))

        return getattr(self, route_value.method_name)(**kwargs)

    def has_route(self, path: str) -> bool:
        """
        Check if the API has a route.

        Keyword arguments:
        path -- The path
        """
        return path in self._route_map

    def respond(self, body: Union[Dict, str], status_code: int, headers: Dict = None) -> Dict:
        '''
        Returns an API Gateway response.

        Keyword arguments:
        body -- The body of the response.
        status_code -- The status code of the response.
        headers -- The headers of the, optional.
        '''

        return {
            'body': body,
            'headers': headers,
            'statusCode': status_code,
        }

    def route_value(self, path: str) -> Route:
        """
        Get the value of a route.

        Keyword arguments:
        path -- The path
        """
        return self._route_map[path]


class ParentAPI(ChildAPI):
    routes: List[Route] = []

    def __init__(self, child_apis: List[ChildAPI]):
        self.child_apis = child_apis

        for child_api in self.child_apis:
            self.routes.extend([Route(path=r.path, method_name=child_api) for r in child_api.routes])

        super().__init__()

    def execute_path(self, path: str, **kwargs):
        """
        Execute a path

        Keyword arguments:
        path -- The path
        """
        if not self.has_route(path):
            return self.respond(body=f"Path not found", status_code=404)

        route_klass = self.route_value(path).method_name

        initialized_obj = route_klass()

        try:
            return initialized_obj.execute_path(path, **kwargs)

        except RequestAttributeError as req_err:
            return self.respond(body=str(req_err), status_code=400)

        except InvalidPathError as inv_err:
            return self.respond(body=str(inv_err), status_code=404)
        
        except Exception as e:
            return self.respond(body="internal error occurred", status_code=500)
