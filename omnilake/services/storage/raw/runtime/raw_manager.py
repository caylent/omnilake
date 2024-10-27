'''
Manages the raw data storage for the runtime
'''
import boto3

from botocore.exceptions import ClientError

from da_vinci.core.logging import Logger
from da_vinci.core.global_settings import setting_value
from da_vinci.core.rest_service_base import SimpleRESTServiceBase, Route

from da_vinci.exception_trap.client import fn_exception_reporter


class RawManager(SimpleRESTServiceBase):
    '''
    Manages the raw data storage for the runtime.
    '''

    def __init__(self):
        '''
        Initializes the raw manager.
        '''
        super().__init__(
            routes=[
                Route(
                    handler=self.delete_entry,
                    method='POST',
                    path='/delete_entry'
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
            ]
        )

        self.raw_bucket = setting_value(namespace='storage', setting_key='raw_entry_bucket')

        self.s3 = boto3.client('s3')

    def check_object_exists(self, bucket, key):
        try:
            self.s3.head_object(Bucket=bucket, Key=key)

            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False

            else:
                raise

    def delete_entry(self, entry_id: str):
        """
        Deletes an entry

        Idempotent

        Keyword arguments:
        entry_id -- The entry ID
        """
        s3_key = f"{entry_id}.txt"

        if not self.check_object_exists(self.raw_bucket, s3_key):
            return self.respond(
                body={"message": "Entry not found"},
                status_code=404
            )

        self.s3.delete_object(
            Bucket=self.raw_bucket,
            Key=s3_key
        )

        return self.respond(
            body={"message": "Entry deleted"},
            status_code=201
        )

    def get_entry(self, entry_id: str):
        """
        Gets an entry

        Keyword arguments:
        entry_id -- The entry ID
        """
        s3_key = f"{entry_id}.txt"

        if not self.check_object_exists(self.raw_bucket, s3_key):
            return self.respond(
                body={"message": "Entry not found"},
                status_code=404
            )

        response = self.s3.get_object(
            Bucket=self.raw_bucket,
            Key=f"{entry_id}.txt"
        )

        return self.respond(
            body={'content': response['Body'].read().decode()},
            status_code=200
        )

    def save_entry(self, entry_id: str, content: str):
        """
        Saves an entry

        Idempotent

        Keyword arguments:
        entry_id -- The entry ID
        content -- The content of the entry 
        """
        self.s3.put_object(
            Bucket=self.raw_bucket,
            Key=f"{entry_id}.txt",
            Body=content
        )

        return self.respond(
            body={"message": "Entry saved"},
            status_code=201
        )


@fn_exception_reporter(function_name='raw_storage_manager', logger=Logger("omnilake.storage.raw_storage_manager"))
def handler(event, context):
    '''
    Handles the raw storage manager lambda function.

    Keyword arguments:
    event -- The event data
    context -- The context data
    '''
    raw_manager = RawManager()

    return raw_manager.handle(event)