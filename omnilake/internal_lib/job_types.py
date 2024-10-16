'''
List of job types
'''

from enum import StrEnum


class JobType(StrEnum):
    """
    Job Type
    """
    ADD_ENTRY = 'ADD_ENTRY'
    CREATE_ARCHIVE = 'CREATE_ARCHIVE'
    DELETE_ENTRY = 'DELETE_ENTRY'
    DELETE_SOURCE = 'DELETE_SOURCE'
    INDEX_ENTRY = 'INDEX_ENTRY'
    INFORMATION_REQUEST = 'INFORMATION_REQUEST'
    PROCESS_ENTRY = 'PROCESS_ENTRY'
    RECALCULATE_VECTOR_TAGS = 'RECALCULATE_VECTOR_TAGS'
    REFRESH_VECTOR_STORES = 'REFRESH_VECTOR_STORES'
    UPDATE_ENTRY = 'UPDATE_ENTRY'

    @staticmethod
    def all():
        return [job_type for job_type in JobType]

    def __str__(self):
        return self.value