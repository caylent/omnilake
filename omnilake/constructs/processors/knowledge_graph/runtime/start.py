'''Kicks off the Knowledge Graph Processor.'''
import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict

from da_vinci.core.immutable_object import ObjectBody
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import ExceptionReporter

from da_vinci.event_bus.client import fn_event_response, EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent

from omnilake.internal_lib.event_definitions import (
    LakeRequestInternalRequestEventBodySchema,
)

from omnilake.tables.jobs.client import JobsClient, JobStatus

from omnilake.constructs.processors.knowledge_graph.runtime.event_definitions import (
    KnowledgeExtractionRequestSchema,
)

from omnilake.constructs.processors.knowledge_graph.tables.knowledge_graph_jobs.client import (
    KnowledgeGraphJob,
    KnowledgeGraphJobClient,
)


_FN_NAME = "omnilake.constructs.processors.knowledge_graph.start"


@fn_event_response(exception_reporter=ExceptionReporter(), function_name=_FN_NAME, logger=Logger(_FN_NAME))
def handler(event: Dict, context: Dict):
    '''
    Kicks off the Knowledge Graph Processor.
    '''
    logging.debug(f'Recieved request: {event}')

    source_event = EventBusEvent.from_lambda_event(event)

    event_body = ObjectBody(
        body=source_event.body,
        schema=LakeRequestInternalRequestEventBodySchema,
    )

    omni_jobs = JobsClient()

    job = omni_jobs.get(job_id=event_body.get("parent_job_id"), job_type=event_body.get("parent_job_type"),
                        consistent_read=True)

    processor_system_job = job.create_child(job_type="LAKE_PROCESSOR_KNOWLEDGE_GRAPH")

    processor_system_job.status = JobStatus.IN_PROGRESS

    processor_system_job.started = datetime.now(tz=utc_tz)

    omni_jobs.put(processor_system_job)

    entries = event_body["entry_ids"]

    processing_jobs = KnowledgeGraphJobClient()

    req_body = event_body["request_body"]

    processing_job = KnowledgeGraphJob(
        configuration=req_body.to_dict(),
        goal=req_body["goal"],
        lake_request_id=event_body["lake_request_id"],
        parent_job_id=processor_system_job.job_id,
        parent_job_type=processor_system_job.job_type,
        remaining_processes=len(entries),
    )

    processing_jobs.put(knowledge_graph_job=processing_job)

    event_publisher = EventPublisher()

    goal = None

    if req_body["knowledge_extraction_include_goal"]:
        goal = req_body["goal"]

    for entry in entries:
        obj_body = ObjectBody(
            body={
                "goal": goal,
                "entry_id": entry,
                "knowledge_graph_processing_id": processing_job.knowledge_graph_processing_id,
                "model_id": req_body.get("knowledge_extraction_model_id"),
                "parent_job_id": processing_job.parent_job_id,
                "parent_job_type": processing_job.parent_job_type,
            },
            schema=KnowledgeExtractionRequestSchema,
        )

        event_publisher.submit(
            event=source_event.next_event(
                event_type=obj_body["event_type"],
                body=obj_body,
            )
        )