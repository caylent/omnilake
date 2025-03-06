import json
import time

from datetime import datetime
from typing import Optional

from omnilake.client.client import OmniLake
from omnilake.client.request_definitions import (
    DescribeChainRequest,
    DescribeJob,
    DescribeLakeRequest,
    GetEntry,
    SubmitChainRequest,
)


from omnilake.client.commands.base import Command


def output_lake_response(omnilake: OmniLake, request_id: str, request_name: str):
    """
    Output the response of a lake request

    Keyword arguments:
    omnilake -- The OmniLake client.
    request_id -- The ID of the request.
    request_name -- The name of the
    """
    request_request_obj = DescribeLakeRequest(lake_request_id=request_id)

    resp = omnilake.request(request_request_obj)

    entry_id = resp.response_body['response_entry_id']

    content_resp = omnilake.request(GetEntry(entry_id=entry_id))

    entry_content = content_resp.response_body['content']

    print(f"Request \"{request_name}\" Response\n=================\n\n{entry_content}")


class ChainCommand(Command):
    command_name='chain'

    description='Execute a chain against OmniLake'

    def __init__(self, omnilake_app_name: Optional[str] = None,
                 omnilake_deployment_id: Optional[str] = None):
        self.omnilake = OmniLake(
            app_name=omnilake_app_name,
            deployment_id=omnilake_deployment_id,
        )

    @classmethod
    def configure_parser(cls, parser):
        chain_parser = parser.add_parser(cls.command_name, help=cls.description)

        chain_parser.add_argument('chain_definition', help='The chain definition file to execute')

        return chain_parser

    def run(self, args):
        """
        Execute the command
        """
        with open(args.chain_definition, 'r') as chain_file:
            loaded_chain_file = json.load(chain_file)
        
        print('Loaded Chain Definition:')

        print(json.dumps(loaded_chain_file, indent=4))

        print('Executing chain')

        omnilake = OmniLake()

        request = SubmitChainRequest(chain=loaded_chain_file)

        chain_create_resp = omnilake.request(request=request)

        job_type = chain_create_resp.response_body["job_type"]

        job_id = chain_create_resp.response_body["job_id"]

        chain_id = chain_create_resp.response_body["chain_request_id"]

        job_describe = DescribeJob(
            job_id=job_id,
            job_type=job_type,
        )

        job_resp = omnilake.request(job_describe)

        job_status = job_resp.response_body['status']

        job_failed = False

        while job_status != 'COMPLETED':
            time.sleep(10)

            job_resp = omnilake.request(job_describe)

            if job_resp.response_body['status'] != job_status:
                job_status = job_resp.response_body['status']

                if job_status == 'FAILED':
                    print(f'Job failed: {job_resp.response_body["status_message"]}')

                    job_failed = True

                    break

                print(f'Job status updated: {job_status}')

        print(f'Final job status: {job_status}')

        started = datetime.fromisoformat(job_resp.response_body['started'])

        ended = datetime.fromisoformat(job_resp.response_body['ended'])

        total_run_time = ended - started

        print(f'Total run time: {total_run_time}')

        if job_failed:
            status_message = job_resp.response_body.get('status_message')

            if status_message:
                print(f'Status Message: {status_message}')

            return

        chain_describe = DescribeChainRequest(
            chain_request_id=chain_id,
        )

        chain_resp = omnilake.request(chain_describe)

        executed_requests = chain_resp.response_body["executed_requests"]

        for request_name in executed_requests:
            output_lake_response(omnilake=omnilake, request_id=executed_requests[request_name], request_name=request_name)