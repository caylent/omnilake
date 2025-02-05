"""
Graph class for the in-memory knowledge graph.
"""
import logging
from typing import List, Optional

import networkx as nx

from community import community_louvain


class Graph:
    def __init__(self, ai_output: Optional[str] = None, graph: Optional[nx.Graph] = None):
        """
        Initialize the graph.

        Keyword Arguments:
        ai_output -- The AI output to initialize the graph with. Optional.
        graph -- The graph to initialize. Optional.
        """
        self.num_connections = 0 

        if graph is not None:
            self._structure = graph

            self.num_connections = self._structure.number_of_edges()

        else:
            self._structure = nx.MultiGraph()

        if ai_output:
            self.add_from_ai_output(ai_output)

    def add(self, source: str, target: str, relationship: str, weight: int = 1):
        """
        Add a relationship to the graph.

        Keyword Arguments:
        source -- The source node.
        target -- The target node.
        relationship -- The relationship between the source and target nodes.
        weight -- The weight of the relationship. Default is 1.
        """
        edges = self._structure.get_edge_data(source, target)

        # Check if the relationship already exists, adjust weight if it does
        if edges:
            for key, data in edges.items():
                if data['relationship'] == relationship:
                    self._structure[source][target][key]['weight'] += weight

                    return

        self.num_connections += 1

        self._structure.add_edge(source, target, relationship=relationship, weight=weight)

    def add_from_ai_output(self, ai_output: str):
        """
        Add relationships to the graph from the AI output. This converts everything to lowercase
        and splits the output by new lines.

        Keyword Arguments:
        ai_output -- The AI output.
        """
        for line in ai_output.split("\n"):
            self.add_from_ai_output_line(line.lower())

    def add_from_ai_output_line(self, line: str):
        """
        Add relationship to the graph from the AI output.

        Example line: "John|friend|Mary" where John is the source, friend is the relationship, and Mary is the target.

        Keyword Arguments:
        line -- The AI output line.
        """
        logging.debug(f'Adding relationship from line: {line}')

        if line == "":
            logging.debug('Line was empty, skipping')

            return

        if "|" not in line:
            logging.debug('Line did not contain |, skipping')

            return

        source, relationship, target = line.split("|")

        self.add(source=source, target=target, relationship=relationship)

    def calculate_community_subgraphs(self) -> List['Graph']:
        """
        Calculate the community subgraphs of the graph using the Louvain method. Returned in order of density.
        """
        if self.num_connections == 0:
            logging.debug('Graph had no connections, returning empty list of subgraphs')

            return []

        # Get communities
        communities = community_louvain.best_partition(self._structure, weight='weight')

        community_data = []

        for community_id in set(communities.values()):
            # Get all nodes in the community
            nodes = [node for node, com in communities.items() if com == community_id]

            # Get subgraph with all edges
            subgraph = Graph(graph=self._structure.subgraph(nodes))

            n_nodes = len(nodes)

            total_weight = sum(d['weight'] for _, _, d in subgraph._structure.edges(data=True))

            density = total_weight / (n_nodes * (n_nodes - 1)) if n_nodes > 1 else 0

            community_data.append((subgraph, density))

        return [g for g, _ in sorted(community_data, key=lambda x: x[1], reverse=True)]

    def filter_by_weight(self, min_weight: int) -> 'Graph':
        """
        Filter the graph by weight and return a new graph with the filtered edges.

        Keyword Arguments:
        min_weight -- The minimum weight to keep.
        """
        if self.num_connections == 0:
            logging.debug('Graph had no connections, returning empty graph.')

            return Graph()

        _raw_structure = self._structure.copy()

        for u, v, data in self._structure.edges(data=True):
            if data['weight'] < min_weight:
                _raw_structure.remove_edge(u, v)

        return Graph(graph=_raw_structure)

    def to_triple_str(self) -> str:
        """
        Convert the graph to a string.
        """
        node_rel_str_list = []

        for u, v, data in self._structure.edges(data=True):
            rel_str = f"{u}|{data['relationship']}|{v}"

            node_rel_str_list.append(rel_str)

        return "\n".join(node_rel_str_list)

    def to_str(self, include_weight: bool = True) -> str:
        """
        Convert the graph to a string.
        """
        node_rel_str_list = []

        for u, v, data in self._structure.edges(data=True):
            rel_str = f"{u} -{data['relationship']}-> {v}"

            if include_weight:
                rel_str = f"{rel_str} (weight: {data['weight']})"

            node_rel_str_list.append(rel_str)

        return "\n".join(node_rel_str_list)