import logging

from typing import Dict

from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.clients import (
    RawStorageManager,
)
from omnilake.internal_lib.naming import EntryResourceName

from omnilake.constructs.processors.knowledge_graph.runtime.event_definitions import (
    KnowledgeAIFilteringCompleteSchema,
    FinalResponseRequestSchema,
)
from omnilake.constructs.processors.knowledge_graph.runtime.graph import Graph

from omnilake.constructs.processors.knowledge_graph.tables.knowledge_graph_jobs.client import (
    KnowledgeGraphJobClient,
)


_FN_NAME = "omnilake.constructs.processors.knowledge_graph.community_filter_complete"


@fn_event_response(exception_reporter=ExceptionReporter(), function_name=_FN_NAME, logger=Logger(_FN_NAME))
def handler(event: Dict, context: Dict):
    '''
    Filters the provided knowledge graph communities.
    '''
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=KnowledgeAIFilteringCompleteSchema,
    )

    kg_jobs = KnowledgeGraphJobClient()

    kg_request_id = event_body["knowledge_graph_processing_id"]

    # Add AI invocation if one was returned
    ai_invocation_id = event_body.get("ai_invocation_id")

    remaining_processes = kg_jobs.add_completed_entry(
        ai_invocation_id=ai_invocation_id,
        entry_attr_name="FilteredEntryIds",
        entry_id=event_body["entry_id"],
        knowledge_graph_processing_id=kg_request_id,
    )

    logging.debug(f'Added entry {event_body["entry_id"]} to knowledge graph job {kg_request_id}.')

    if remaining_processes != 0:
        logging.debug(f'Knowledge graph job {kg_request_id} still has {remaining_processes} remaining processes.')

        return

    kg_job = kg_jobs.get(knowledge_graph_request_id=kg_request_id, consistent_read=True)

    # Create final entry for the knowledge graph

    event_bus = EventPublisher()
    # Submit for final response

    raw_storage = RawStorageManager()

    final_graph = Graph()

    for filtered_entry_id in kg_job.filtered_entry_ids:
        storage_resp = raw_storage.get_entry(entry_id=filtered_entry_id)

        final_graph.add_from_ai_output(storage_resp.response_body["content"])

    community_group_entry = raw_storage.create_entry(
        content=final_graph.to_str(include_weight=False),
        sources=[str(EntryResourceName(resource_id=e_entry)) for e_entry in kg_job.filtered_entry_ids],
    )

    kg_job_config = kg_job.configuration

    kg_job.stage = "RESPONSE"

    kg_jobs.put(knowledge_graph_job=kg_job)

    final_req_body = ObjectBody(
        body={
            "entry_id": community_group_entry.response_body["entry_id"],
            "goal": kg_job.goal,
            "knowledge_graph_processing_id": kg_job.knowledge_graph_processing_id,
            "model_id": kg_job_config.get("response_model_id"),
            "parent_job_id": kg_job.parent_job_id,
            "parent_job_type": kg_job.parent_job_type,
        },
        schema=FinalResponseRequestSchema,
    )

    event_bus.submit(
        event=source_event.next_event(
            body=final_req_body.to_dict(),
            event_type=final_req_body["event_type"],
        )
    )