"""
Handles the completion of an extraction process.
"""

import logging

from typing import Dict, List

from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.clients import RawStorageManager
from omnilake.internal_lib.naming import EntryResourceName

from omnilake.constructs.processors.knowledge_graph.runtime.event_definitions import (
    KnowledgeAIFilteringRequestSchema,
    KnowledgeExtractionCompleteSchema,
)
from omnilake.constructs.processors.knowledge_graph.runtime.graph import Graph

from omnilake.constructs.processors.knowledge_graph.tables.knowledge_graph_jobs.client import (
    KnowledgeGraphJobClient,
)


def group_graphs_by_num_connections(sub_graphs: List[Graph], group_max_size: int) -> List[List[Graph]]:
    '''
    Groups the graphs by the number of connections they have.

    Keyword Arguments:
    graphs -- The graphs to group.
    group_max_size -- The maximum size of each group.
    '''
    groups = []

    current_group = []

    current_sum = 0
    
    for sub_graph in sorted(sub_graphs, key=lambda g: g.num_connections, reverse=True):
        if current_sum + sub_graph.num_connections <= group_max_size:
            current_group.append(sub_graph)

            current_sum += sub_graph.num_connections

        else:
            if current_group:
                groups.append(current_group)

            current_group = [sub_graph]

            current_sum = sub_graph.num_connections
    
    if current_group:
        groups.append(current_group)
    
    return groups


def load_fully_extracted_graph(extraction_entry_ids: List[str]) -> Graph:
    '''
    Loads the fully extracted graph from the extraction entry ids.
    '''
    graph = Graph()

    raw_storage = RawStorageManager()

    for entry_id in extraction_entry_ids:
        raw_stor_entry = raw_storage.get_entry(entry_id=entry_id)

        entry_content = raw_stor_entry.response_body["content"]

        graph.add_from_ai_output(entry_content)

    return graph


_FN_NAME = "omnilake.constructs.processors.knowledge_graph.extraction_complete"


@fn_event_response(exception_reporter=ExceptionReporter(), function_name=_FN_NAME, logger=Logger(_FN_NAME))
def handler(event: Dict, context: Dict):
    '''
    Handles the completion of an extraction process.
    '''
    logging.debug(f'Received request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=KnowledgeExtractionCompleteSchema,
    )

    kg_jobs = KnowledgeGraphJobClient()

    kg_request_id = event_body["knowledge_graph_processing_id"]

    # Add AI invocation if one was returned
    ai_invocation_id = event_body.get("ai_invocation_id")

    remaining_processes = kg_jobs.add_completed_entry(
        ai_invocation_id=ai_invocation_id,
        entry_id=event_body["entry_id"],
        knowledge_graph_processing_id=kg_request_id,
    )

    logging.debug(f'Added entry {event_body["entry_id"]} to knowledge graph job {kg_request_id}.')

    if remaining_processes != 0:
        logging.debug(f'Knowledge graph job {kg_request_id} still has {remaining_processes} remaining processes.')

        return

    kg_job = kg_jobs.get(knowledge_graph_request_id=kg_request_id, consistent_read=True)

    event_bus = EventPublisher()

    kg_job_config = kg_job.configuration

    # Load the fully extracted graph
    fully_extracted_graph = load_fully_extracted_graph(kg_job.extracted_entry_ids)

    logging.debug(f'Extracted graph had {fully_extracted_graph.num_connections} total connections.')

    # Check for weight threshold, filter the graph from that
    min_considered_weight = kg_job_config['minimally_considered_weight']

    if min_considered_weight > 1:
        fully_extracted_graph = fully_extracted_graph.filter_by_weight(min_considered_weight)

    raw_storage = RawStorageManager()

    min_connections_for_community = kg_job_config['community_filtering_threshold_min']

    # Check for the connection/community threshold, if not met, save the final connections and send to 
    # final response
    if fully_extracted_graph.num_connections < min_connections_for_community:
        logging.info(f'Graph had {fully_extracted_graph.num_connections} connections, below threshold of {min_connections_for_community} for splitting into communities.')

        # Save the final connections to the raw storage
        whole_graph_entry = raw_storage.create_entry(
            content=fully_extracted_graph.to_str(include_weight=False),
            sources=[str(EntryResourceName(resource_id=e_entry)) for e_entry in kg_job.extracted_entry_ids],
        )

        kg_job.remaining_processes = 1

        kg_jobs.put(knowledge_graph_job=kg_job)

        goal = None

        if kg_job_config["ai_filter_include_goal"]:
            goal = kg_job.goal

        single_filter_body = ObjectBody(
            body={
                "goal": goal,
                "entry_id": whole_graph_entry.response_body["entry_id"],
                "knowledge_graph_processing_id": kg_request_id,
                "model_id": kg_job_config["ai_filter_model_id"],
                "parent_job_id": kg_job.parent_job_id,
                "parent_job_type": kg_job.parent_job_type,
            },
            schema=KnowledgeAIFilteringRequestSchema,
        )

        event_bus.submit(
            event=source_event.next_event(
                body=single_filter_body.to_dict(),
                event_type=single_filter_body["event_type"],
            )
        )

        logging.info(f'Submitted final response for knowledge graph {kg_request_id}.')

        return

    # Grab the community % from the provided config in the kg job
    top_n_communities = kg_job_config['top_n_communities']

    # Execute the AI filtering by sending the communities to be filtered
    communities = fully_extracted_graph.calculate_community_subgraphs()

    logging.debug(f'Calculated {len(communities)} communities.')

    include_num_communities = int(len(communities) * top_n_communities)

    logging.debug(f'Calculated {len(communities)} communities, sending top {include_num_communities} to AI for filtering.')

    included_communities = communities[:include_num_communities]

    # Group the communities by the number of connections they have to be more efficient
    community_groups = group_graphs_by_num_connections(
        group_max_size=kg_job_config['community_filtering_max_group_size'],
        sub_graphs=included_communities,
    )

    kg_job.remaining_processes = len(community_groups)

    kg_job.stage = "FILTERING"

    kg_jobs.put(knowledge_graph_job=kg_job)

    for community_group in community_groups:
        group_content = ""

        for community in community_group:
            group_content += "\n" + community.to_str(include_weight=True)

        community_group_entry = raw_storage.create_entry(
            content=group_content,
            sources=[str(EntryResourceName(resource_id=e_entry)) for e_entry in kg_job.extracted_entry_ids],
        )

        goal = None

        if kg_job_config["ai_filter_include_goal"]:
            goal = kg_job.goal

        ai_filter_req_body = ObjectBody(
            body={
                "goal": goal,
                "entry_id": community_group_entry.response_body["entry_id"],
                "knowledge_graph_processing_id": kg_request_id,
                "model_id": kg_job_config["ai_filter_model_id"],
                "parent_job_id": kg_job.parent_job_id,
                "parent_job_type": kg_job.parent_job_type,
            },
            schema=KnowledgeAIFilteringRequestSchema,
        )

        event_bus.submit(
            event=source_event.next_event(
                body=ai_filter_req_body.to_dict(),
                event_type=ai_filter_req_body["event_type"],
            )
        )

        logging.info(f'Submitted AI filtering for community entry {community_group_entry.response_body["entry_id"]}.')