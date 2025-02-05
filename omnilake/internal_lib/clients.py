'''
Definitions of internal clients
'''
import logging

from datetime import datetime
from typing import Dict, List, Optional, Set, Union

from da_vinci.core.client_base import RESTClientBase

from da_vinci.core.immutable_object import (
    ObjectBody,
    ObjectBodySchema,
    SchemaAttribute,
    SchemaAttributeType,
)


class AIStatisticSchema(ObjectBodySchema):
    """
    AI statistic schema

    Attributes:
    job_id -- The job ID
    job_type -- The job type
    model_id -- The model ID
    model_parameters -- The model parameters, Optional
    resulting_entry_id -- The resulting entry ID
    total_input_tokens -- The total input tokens
    total_output_tokens -- The total output tokens
    """
    attributes = [
        SchemaAttribute(name="job_id", type=SchemaAttributeType.STRING),
        SchemaAttribute(name="job_type", type=SchemaAttributeType.STRING),
        SchemaAttribute(name="invocation_id", type=SchemaAttributeType.STRING, required=False),
        SchemaAttribute(name="model_id", type=SchemaAttributeType.STRING),
        SchemaAttribute(name="model_parameters", type=SchemaAttributeType.OBJECT, required=False),
        SchemaAttribute(name="resulting_entry_id", type=SchemaAttributeType.STRING, required=False),
        SchemaAttribute(name="total_input_tokens", type=SchemaAttributeType.NUMBER),
        SchemaAttribute(name="total_output_tokens", type=SchemaAttributeType.NUMBER),
    ]


class AIStatisticsCollector(RESTClientBase):
    '''
    AI statistics collector client
    '''
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        '''
        Initializes the AI statistics collector client

        Keyword arguments:
        app_name -- The app name
        deployment_id -- The deployment
        '''
        super().__init__(
            app_name=app_name,
            deployment_id=deployment_id,
            resource_name='ai_statistics_collector'
        )

    def publish(self, statistic: Union[ObjectBody, Dict]):
        '''
        Collects AI statistics

        Keyword arguments:
        statistic -- The statistic
        '''
        logging.debug(f"Collecting AI statistic: {statistic}")

        # Validate the statistic against the schema
        if isinstance(statistic, ObjectBody):
            body = statistic.map_to(new_schema=AIStatisticSchema)

        else:
            body = ObjectBody(body=statistic, schema=AIStatisticSchema)

        return self.post(path='/', body=body.to_dict())


class RawStorageManager(RESTClientBase):
    '''
    Raw storage manager client
    '''

    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        '''
        Initializes the raw storage manager client

        Keyword arguments:
        app_name -- The app name
        deployment_id -- The deployment
        '''
        super().__init__(
            app_name=app_name,
            deployment_id=deployment_id,
            resource_name='raw_storage_manager'
        )

    def create_entry(self, content: str, sources: Union[List[str], Set[str]], effective_on: Union[datetime, str] = None):
        '''
        Creates an entry from scratch, manages the entries table and the raw entry bucket

        Keyword arguments:
        content -- The content of the entry
        effective_on -- The effective date of the entry
        sources -- The sources of the entry
        '''
        effective_on_str = effective_on

        if isinstance(effective_on, datetime):
            effective_on_str = effective_on.isoformat()

        return self.post(
            path='/create_entry',
            body={
                'content': content,
                'sources': list(sources),
                'effective_on': effective_on_str,
            }
        )

    def delete_entry(self, entry_id: str):
        '''
        Deletes an entry

        Idempotent

        Keyword arguments:
        entry_id -- The entry ID
        '''
        return self.post(path='/delete_entry', body={'entry_id': entry_id})

    def get_entry(self, entry_id: str):
        '''
        Gets an entry

        Keyword arguments:
        entry_id -- The entry ID
        '''
        return self.post(path='/get_entry', body={'entry_id': entry_id})

    def save_entry(self, entry_id: str, content: str):
        '''
        Saves an entry

        Keyword arguments:
        entry_id -- The entry ID
        entry -- The entry
        '''
        return self.post(
            path='/save_entry',
            body={
                'entry_id': entry_id,
                'content': content
            }
        )