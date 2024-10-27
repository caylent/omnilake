import logging

from datetime import datetime, UTC as utc_tz
from typing import Dict

from da_vinci.core.global_settings import setting_value
from da_vinci.core.logging import Logger

from da_vinci.exception_trap.client import fn_exception_reporter

from da_vinci.event_bus.client import EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent


from omnilake.tables.jobs.client import Job, JobsClient, JobStatus
from omnilake.tables.vector_stores.client import VectorStoresClient

from omnilake.services.storage.vector.runtime.vector_rebalancer import VectorStoreRebalancing


@fn_exception_reporter(function_name='vector_rebalance_checker', logger=Logger('omnilake.storage.vector.vector_rebalance_checker'))
def rebalance_checker(event: Dict, context: Dict):
    """
    Check for vector stores that should be rebalanced based on setting definitions.
    """
    jobs_client = JobsClient()

    job = Job(
        job_type='VECTOR_REBALANCING_CHECK',
        started=datetime.now(tz=utc_tz),
        status=JobStatus.IN_PROGRESS,
    )

    jobs_client.put(job)

    max_entries_rebalance_threshold = setting_value(namespace="vector_storage", setting_key="max_entries_rebalance_threshold")

    max_entries_per_vector_store = setting_value(namespace="vector_storage", setting_key="max_entries_per_vector")

    min_inside_threshold = (max_entries_per_vector_store * (max_entries_rebalance_threshold * 0.01))

    vector_stores = VectorStoresClient()

    event_publisher = EventPublisher()

    # TODO: Handle this better with table scan definition
    for vector_store in vector_stores.all():
        if vector_store.total_entries > min_inside_threshold:
            event_publisher.submit(
                event=EventBusEvent(
                    body=VectorStoreRebalancing(
                        archive_id=vector_store.archive_id,
                        vector_store_id=vector_store.vector_store_id,
                    ).to_dict(),
                    event_type='vector_store_rebalancing',
                )
            )

    job.status = JobStatus.COMPLETED

    job.ended = datetime.now(tz=utc_tz)

    jobs_client.put(job)