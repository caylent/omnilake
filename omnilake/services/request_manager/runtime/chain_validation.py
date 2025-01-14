"""
Handles dependency validation of a chain declaration
"""
import logging

from typing import List


class ChainNode:
    def __init__(self, name: str, conditional: bool = False, direct_references: List[str] = None,
                 on_failure_reference: str = None, on_success_reference: str = None, referenced_by: List[str] = None):
        """
        Chain Node used by validation tree to validate chain configuration.

        Keyword arguments:
        name -- Name of the node.
        conditional -- Boolean to indicate if the node is conditional
        direct_references -- List of node names that reference the node
        on_failure_reference -- Name of the node to reference on failure
        on_success_reference -- Name of the node to reference on success
        referenced_by -- List of node names that the node references
        """
        self.direct_references = direct_references or []

        self.conditional = conditional

        self._referenced_by = referenced_by or []

        self.name = name

        self.on_failure_reference = on_failure_reference

        self.on_success_reference = on_success_reference

    def add_referenced_by(self, parent: str):
        """
        Add a reference to the node.
        """
        if parent in self._referenced_by:
            return

        self._referenced_by.append(parent)

    def all_next_steps(self):
        """
        Return all paths for the node.
        """
        next_node_names = self._referenced_by

        if self.on_failure_reference:
            next_node_names.append(self.on_failure_reference)

        if self.on_success_reference:
            next_node_names.append(self.on_success_reference)

        return next_node_names

    def unchecked_paths(self):
        """
        Return the next unchecked path node name.
        """
        if not self._unchecked_paths:
            return None

        return self._unchecked_paths

    def to_dict(self):
        """
        Return the node as a dictionary.
        """
        return {
            "name": self.name,
            "direct_references": self.direct_references,
            "on_failure_reference": self.on_failure_reference,
            "on_success_reference": self.on_success_reference,
            "referenced_by": self._referenced_by,
        }

    def valid(self):
        """
        Returns True if there are no unchecked paths.
        """
        return not self._unchecked_paths


class ChainConfigurationValidationError(Exception):
    def __init__(self, message: str):
        super().__init__(f"invalid chain configuration {message}")


class ValidateChain:
    def __call__(self, chain_nodes: List[ChainNode]):
        """
        Call the chain validation.

        Keyword arguments:
        chain_nodes -- List of chain nodes to validate
        """
        self._already_loaded_nodes = []

        self._chain_node_by_name = {node.name: node for node in chain_nodes}

        for node in chain_nodes:
            if node.name in self._already_loaded_nodes:
                raise ChainConfigurationValidationError(f"duplicate lake_request {node.name} found")

            for reference in node.direct_references:
                if reference not in self._chain_node_by_name:
                    raise ChainConfigurationValidationError(f"Node {reference} is not defined")

                # Add forward reference
                self._chain_node_by_name[reference].add_referenced_by(node.name)

            self._already_loaded_nodes.append(node.name)

            self._chain_node_by_name[node.name] = node

        self._validate_paths()

    def _validate_paths(self):
        """
        Validate all possible paths of the chain.
        """
        entry_points = [node for node in self._chain_node_by_name.values() if not node._referenced_by]
        
        for entry in entry_points:
            self._walk_path(entry, [])

    def _walk_path(self, node: ChainNode, seen_path: List[str]):
        """
        Walk the path of the node and validate the chain configuration.

        Keyword arguments:
        node -- Node to walk
        seen_path -- List of node names that have been seen
        """
        logging.debug(f"walking path for {node.name} with seen path {seen_path}")

        if node.name in seen_path:
            raise ChainConfigurationValidationError(f"cycle detected in path {seen_path + [node.name]}")
        
        current_path = seen_path + [node.name]
        
        # Walk direct references
        for ref in node.direct_references:
            self._walk_path(self._chain_node_by_name[ref], current_path)
            
        # Walk conditional paths
        if node.conditional:
            if node.on_success_reference:
                self._walk_path(self._chain_node_by_name[node.on_success_reference], current_path)

            if node.on_failure_reference:
                self._walk_path(self._chain_node_by_name[node.on_failure_reference], current_path)