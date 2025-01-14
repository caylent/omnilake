"""
Mini API Constructor Library

Makes it easy to create this monster API while making the code a bit more maintainable.
"""
import logging
import traceback

from dataclasses import dataclass
from typing import Dict, List, Union, Type

from da_vinci.core.immutable_object import (
    MissingAttributeError,
    ObjectBody,
    ObjectBodySchema,
)

from da_vinci.exception_trap.client import ExceptionReporter


class InvalidPathError(ValueError):
    def __init__(self, path: str):
        super().__init__(f"Path {path} not found in route map.")


@dataclass
class Route:
    path: str
    method_name: str
    request_body_schema: Type[ObjectBodySchema] = None


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

        logging.debug(f"Executing path {path} with kwargs {kwargs}")

        if route_value.request_body_schema:
            logging.info(f"Request body expected for route {path}")

            obj_body = ObjectBody(body=kwargs, schema=route_value.request_body_schema)

            return getattr(self, route_value.method_name)(obj_body)

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

        except MissingAttributeError as req_err:
            return self.respond(body=str(req_err), status_code=400)

        except InvalidPathError as inv_err:
            return self.respond(body=str(inv_err), status_code=404)
        
        except Exception as e:
            reporter = ExceptionReporter()

            reporter.report(
                function_name="omnilake_api",
                exception=str(e),
                exception_traceback=traceback.format_exc(),
                originating_event=kwargs
            )

            return self.respond(body="internal error occurred", status_code=500)