'''
Mini API Constructor Library

Makes it easy to create this monster API while making the code a bit more maintainable.
'''
from dataclasses import dataclass
from typing import Dict, List, Union


class InvalidPathError(ValueError):
    def __init__(self, path: str):
        super().__init__(f"Path {path} not found in route map.")


@dataclass
class Route:
    path: str
    method_name: str


class ChildAPI:
    routes: List[Route] = []

    def  __init__(self):
        self._route_map = {route.path: route.method_name for route in self.routes}

    def execute_path(self, path: str, **kwargs):
        """
        Execute a path

        Keyword arguments:
        path -- The path
        """
        if path not in self._route_map:
            raise InvalidPathError(path)

        return getattr(self, self._route_map[path])(**kwargs)

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

    def route_value(self, path: str) -> str:
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
            self.routes.extend([Route(r.path, child_api) for r in child_api.routes])

        super().__init__()

    def execute_path(self, path: str, **kwargs):
        """
        Execute a path

        Keyword arguments:
        path -- The path
        """
        if not self.has_route(path):
            return self.respond(body=f"Path not found", status_code=404)

        route_klass = self.route_value(path)

        initialized_obj = route_klass()

        return initialized_obj.execute_path(path, **kwargs)