'''
Resource Naming Conventions
'''

from typing import List, Union


class CompositeResourceID:
    def __init__(self, key_part_names: List[str], resource_id: str, separator: str = "/"):
        """
        Composite resource ID

        Keyword arguments:
        key_part_names -- The key part names
        resource_id -- The resource ID
        separator -- The separator
        """
        self.resource_id = resource_id

        self.separator = separator

        self.resource_id_parts = self.resource_id.split(self.separator)

        for i, key_part_name in enumerate(key_part_names):
            setattr(self, key_part_name, self.resource_id_parts[i])

    def __str__(self):
        return self.resource_id


class ResourceNameObject:
    def __init__(self, resource_type: str, resource_id: Union[CompositeResourceID, str]):
        """
        Resource name object

        Keyword arguments:
        resource_type -- The resource type
        resource_id -- The resource ID
        """
        # Check if the resource ID is actually a full ORN and handle it.
        # It's a bit of magic, so I don't have to think about these things getting converted back and forth.
        if resource_id.startswith('orn'):
            return self.from_resource_name(resource_id)

        self.resource_type = resource_type

        self.resource_id = resource_id

    def __str__(self):
        return f"orn::{self.resource_type}::{self.resource_id}"

    @staticmethod
    def from_resource_name(resource_name: str) -> 'ResourceNameObject':
        """
        Return a resource name object from a resource name string

        Keyword arguments:
        resource_name -- The resource name
        """
        pieces = resource_name.split("::")

        if pieces[0] != "orn":
            raise ValueError(f"Invalid resource name: {resource_name}")

        resource_type = pieces[1]

        resource_id = pieces[2]

        return OmniLakeResourceName()(resource_type, resource_id)


class ArchiveResourceName(ResourceNameObject):
    def __init__(self, resource_id: str):
        """
        Archive resource name

        Keyword arguments:
        resource_id -- The archive ID
        """
        super().__init__(
            resource_type="archive",
            resource_id=resource_id
        )


class EntryResourceName(ResourceNameObject):
    def __init__(self, resource_id: str):
        """
        Entry resource name

        Keyword arguments:
        resource_id -- The entry ID
        """
        super().__init__(
            resource_type="entry",
            resource_id=resource_id
        )


class JobResourceName(ResourceNameObject):
    def __init__(self, resource_id: str):
        """
        Job resource name

        Keyword arguments:
        resource_id -- The job ID
        """
        super().__init__(
            resource_type="job",
            resource_id=CompositeResourceID(
                key_part_names=["job_type", "job_id"],
                resource_id=resource_id,
            )
        )

class SourceResourceName(ResourceNameObject):
    def __init__(self, resource_id: str):
        """
        Source resource name

        Keyword arguments:
        resource_id -- The source ID
        """
        super().__init__(
            resource_type="source",
            resource_id=resource_id
        )


class OmniLakeResourceName:
    __orn_type_map = {
        "archive": ArchiveResourceName,
        "entry": EntryResourceName,
        "job": JobResourceName,
        "source": SourceResourceName,
    }

    def __call__(self, resource_type: str, resource_id: str) -> ResourceNameObject:
        """
        Get a resource name object

        Keyword arguments:
        resource_type -- The resource type
        resource_id -- The resource ID
        """
        if resource_type not in self.__orn_type_map:
            raise ValueError(f"Invalid resource type: {resource_type}")

        return self.__orn_type_map[resource_type](resource_id)

    @staticmethod
    def from_string(resource_name: str) -> ResourceNameObject:
        """
        Get a resource name object from a string

        Keyword arguments:
        resource_name -- The resource name
        """
        parts = resource_name.split("::")

        if len(parts) != 3:
            raise ValueError(f"Invalid resource name '{resource_name}'")

        resource_type = parts[1]

        resource_id = parts[2]

        return OmniLakeResourceName()(resource_type, resource_id)