from da_vinci.core.client_base import RESTClientBase, RESTClientResponse

import omnilake.client.request_definitions as reqs


class OmniLake(RESTClientBase):
    def __init__(self, app_name: str = 'omnilake', deployment_id: str = 'dev'):
        super().__init__(
            resource_name='omnilake-public-api',
            app_name=app_name,
            deployment_id=deployment_id
        )

    def add_entry(self, entry_request: reqs.AddEntry) -> RESTClientResponse:
        """
        Add an entry to omnilake.

        Keyword arguments:
        entry_request -- The request to add an entry to omnilake.
        """
        return self.post(
            body=entry_request.to_dict(),
            path='/add_entry'
        )

    def add_source(self, source_request: reqs.AddSource) -> RESTClientResponse:
        """
        Add a source to omnilake.

        Keyword arguments:
        source_request -- The request to add a source to omnilake.
        """
        return self.post(
            body=source_request.to_dict(),
            path='/add_source'
        )

    def create_archive(self, create_archive_request: reqs.CreateArchive) -> RESTClientResponse:
        """
        Create an archive in omnilake.

        Keyword arguments:
        create_archive_request -- The request to create an archive in omnilake.
        """
        return self.post(
            body=create_archive_request.to_dict(),
            path='/create_archive'
        )

    def delete_entry(self, delete_entry_request: reqs.DeleteEntry) -> RESTClientResponse:
        """
        Delete an entry from omnilake.

        Keyword arguments:
        delete_entry_request -- The request to delete an entry from omnilake.
        """
        return self.post(
            body=delete_entry_request.to_dict(),
            path='/delete_entry'
        )

    def delete_source(self, delete_source_request: reqs.DeleteSource) -> RESTClientResponse:
        """
        Delete a source from omnilake.

        Keyword arguments:
        delete_source_request -- The request to delete a source from omnilake.
        """
        return self.post(
            body=delete_source_request.to_dict(),
            path='/delete_source'
        )

    def describe_entry(self, describe_entry_request: reqs.DescribeEntry) -> RESTClientResponse:
        """
        Describe an entry in omnilake.

        Keyword arguments:
        describe_entry_request -- The request to describe an entry in omnilake.
        """
        return self.post(
            body=describe_entry_request.to_dict(),
            path='/describe_entry'
        )

    def describe_job(self, describe_job_request: reqs.DescribeJob) -> RESTClientResponse:
        """
        Describe a job in omnilake.

        Keyword arguments:
        describe_job_request -- The request to describe a job in omnilake.
        """
        return self.post(
            body=describe_job_request.to_dict(),
            path='/describe_job'
        )

    def describe_source(self, describe_source_request: reqs.DescribeSource) -> RESTClientResponse:
        """
        Describe a source in omnilake.

        Keyword arguments:
        describe_source_request -- The request to describe a source in omnilake.
        """
        return self.post(
            body=describe_source_request.to_dict(),
            path='/describe_source'
        )

    def describe_request(self, describe_request: reqs.DescribeRequest) -> RESTClientResponse:
        """
        Describe a request in omnilake.

        Keyword arguments:
        get_response_request -- The request to get a response from omnilake.
        """
        return self.post(
            body=describe_request.to_dict(),
            path='/describe_request'
        )

    def get_entry(self, get_entry_request: reqs.GetEntry) -> RESTClientResponse:
        """
        Get an entry from omnilake.

        Keyword arguments:
        get_entry_request -- The request to get an entry from omnilake.
        """
        return self.post(
            body=get_entry_request.to_dict(),
            path='/get_entry'
        )

    def index_entry(self, index_entry_req: reqs.IndexEntry) -> RESTClientResponse:
        """
        Index an entry into an archive in omnilake.

        Keyword arguments:
        copy_entry_request -- The request to copy an entry in omnilake.
        """
        return self.post(
            body=index_entry_req.to_dict(),
            path='/index_entry'
        )
    
    def request_information(self, information_request: reqs.InformationRequest) -> RESTClientResponse:
        """
        Request information from omnilake.

        Keyword arguments:
        request_information_request -- The request to get information from omnilake.
        """
        return self.post(
            body=information_request.to_dict(),
            path='/request_information'
        )

    def update_entry(self, update_entry_request: reqs.UpdateEntry) -> RESTClientResponse:
        """
        Update an entry in omnilake.

        Keyword arguments:
        update_entry_request -- The request to update an entry in omnilake.
        """
        return self.post(
            body=update_entry_request.to_dict(),
            path='/update_entry'
        )