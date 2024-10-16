'''
Definitions of internal clients
'''

from typing import Optional

from da_vinci.core.client_base import RESTClientBase


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