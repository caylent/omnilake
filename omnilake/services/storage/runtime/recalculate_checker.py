import logging

from datetime import datetime, timedelta, UTC as utc_tz
from typing import Dict

from da_vinci.core.global_settings import setting_value
from da_vinci.core.logging import Logger

from da_vinci.event_bus.client import EventPublisher
from da_vinci.event_bus.event import Event as EventBusEvent
from da_vinci.exception_trap.client import fn_exception_reporter

from omnilake.internal_lib.event_definitions import VectorStoreTagRecalculation

from omnilake.tables.vector_stores.client import VectorStoresScanDefinition, VectorStoresClient



@fn_exception_reporter(function_name='recalculate_checker', logger=Logger('omnilake.storage.recalculate_checker'))
def recalculate_checker(event: Dict, context: Dict):
    """
    Check for vector stores that have not been recalculated in a while and submit them for recalculation.

    Keyword arguments:
    event -- The event that triggered the function.
    context -- The context of the function.
    """
    recalculation_freq_days = setting_value(namespace='storage', setting_key='tag_recalculation_frequency')

    older_than = datetime.now(tz=utc_tz) - timedelta(days=recalculation_freq_days)

    vector_stores_client = VectorStoresClient()

    event_bus = EventPublisher()

    scan_def = VectorStoresScanDefinition()

    scan_def.add('total_entries_last_calculated', 'less_than', older_than)

    # TODO: Handle this better with table scan definition
    for page in vector_stores_client.scanner(scan_definition=scan_def):
        for vector_store in page:
            last_checked = vector_store.total_entries_last_calculated.replace(tzinfo=utc_tz)

            if last_checked < older_than:
                logging.info(f'Vector store {vector_store.vector_store_id} needs recalculation')

                event = VectorStoreTagRecalculation(
                    archive_id=vector_store.archive_id,
                    vector_store_id=vector_store.vector_store_id,
                )

                logging.debug(f'Submitting event: {event}')

                event_bus.submit(event=EventBusEvent(body=event.to_dict(), event_type='recalculate_vector_tags'))