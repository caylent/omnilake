'''
Manages the raw data storage for the runtime
'''
import logging

import boto3

from datetime import datetime
from typing import Dict, List

from botocore.exceptions import ClientError

from da_vinci.core.logging import Logger
from da_vinci.core.global_settings import setting_value
from da_vinci.core.rest_service_base import SimpleRESTServiceBase, Route

from da_vinci.exception_trap.client import fn_exception_reporter, ExceptionReporter

from omnilake.tables.entries.client import Entry, EntriesClient


_FN_NAME = "omnilake.service.raw_storage_manager"


class RawManager(SimpleRESTServiceBase):
    '''
    Manages the raw data storage for the runtime.
    '''

    def __init__(self):
        '''
        Initializes the raw manager.
        '''
        super().__init__(
            exception_function_name=_FN_NAME,
            exception_reporter=ExceptionReporter(),
            routes=[
                Route(
                    handler=self.create_entry,
                    method='POST',
                    path='/create_entry'
                ),
                Route(
                    handler=self.delete_entry,
                    method='POST',
                    path='/delete_entry'
                ),
                Route(
                    handler=self.describe_entry,
                    method='POST',
                    path='/describe_entry'
                ),
                Route(
                    handler=self.get_entry,
                    method='POST',
                    path='/get_entry'
                ),
                Route(
                    handler=self.save_entry,
                    method='POST',
                    path='/save_entry'
                )
            ],
        )

        self.raw_bucket = setting_value(namespace='omnilake::storage', setting_key='raw_entry_bucket')

        self.s3 = boto3.client('s3')

    def check_object_exists(self, bucket: str, key: str):
        try:
            self.s3.head_object(Bucket=bucket, Key=key)

            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False

            else:
                raise

    def create_entry(self, content: str, sources: List[str], effective_on: str = None):
        """
        Creates an entry

        Keyword arguments:
        content -- The content of the entry
        """
        if effective_on:
            effective_on = datetime.fromisoformat(effective_on)

        entry = Entry(
            char_count=len(content),
            content_hash=Entry.calculate_hash(content),
            effective_on=effective_on,
            sources=set(sources),
        )

        entries = EntriesClient()

        entries.put(entry=entry)

        entry_id = entry.entry_id

        logging.debug(f"Created new entry with ID: {entry_id}")

        self.s3.put_object(
            Bucket=self.raw_bucket,
            Key=entry_id,
            Body=content,
        )

        return self.respond(
            body={"entry_id": entry_id},
            status_code=201
        )

    def delete_entry(self, entry_id: str):
        """
        Deletes an entry

        Idempotent

        Keyword arguments:
        entry_id -- The entry ID
        """
        if not self.check_object_exists(bucket=self.raw_bucket, key=entry_id):
            return self.respond(
                body={"message": "Entry not found"},
                status_code=404
            )

        self.s3.delete_object(
            Bucket=self.raw_bucket,
            Key=entry_id
        )

        return self.respond(
            body={"message": "Entry deleted"},
            status_code=201
        )

    def describe_entry(self, entry_id: str):
        """
        Describes an entry

        Keyword arguments:
        entry_id -- The entry ID
        """
        entries = EntriesClient()

        entry = entries.get(entry_id=entry_id)

        if not entry:
            return self.respond(
                body={"message": "Entry not found"},
                status_code=404
            )

        return self.respond(
            body=entry.to_dict(json_compatible=True),
            status_code=200
        )

    def get_entry(self, entry_id: str):
        """
        Gets an entry

        Keyword arguments:
        entry_id -- The entry ID
        """
        if not self.check_object_exists(bucket=self.raw_bucket, key=entry_id):
            return self.respond(
                body={"message": "Entry not found"},
                status_code=404
            )

        response = self.s3.get_object(
            Bucket=self.raw_bucket,
            Key=entry_id
        )

        return self.respond(
            body={'content': response['Body'].read().decode()},
            status_code=200
        )

    def save_entry(self, entry_id: str, content: str):
        """
        Saves an entry

        Keyword arguments:
        entry_id -- The entry ID
        content -- The content of the entry 
        """
        self.s3.put_object(
            Bucket=self.raw_bucket,
            Key=entry_id,
            Body=content
        )

        return self.respond(
            body={"message": "Entry saved"},
            status_code=201
        )


@fn_exception_reporter(function_name=_FN_NAME, logger=Logger(_FN_NAME))
def handler(event: Dict, context: Dict):
    '''
    Handles the raw storage manager lambda function.

    Keyword arguments:
    event -- The event data
    context -- The context data
    '''
    raw_manager = RawManager()

    return raw_manager.handle(event)