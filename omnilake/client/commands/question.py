import time

from datetime import datetime
from typing import Optional

from omnilake.client.client import OmniLake
from omnilake.client.request_definitions import (
    DescribeJob,
    DirectResponseConfig,
    GetEntry,
    DescribeLakeRequest,
    SubmitLakeRequest,
    SummarizationProcessor,
    VectorLookup,
)

from omnilake.internal_lib.ai import ModelIDs


from omnilake.client.commands.base import Command


class QuestionCommand(Command):
    command_name='question'

    description='Answer a question from the user about the project'

    def __init__(self, omnilake_app_name: Optional[str] = None,
                 omnilake_deployment_id: Optional[str] = None):
        self.omnilake = OmniLake(
            app_name=omnilake_app_name,
            deployment_id=omnilake_deployment_id,
        )

    @classmethod
    def configure_parser(cls, parser):
        question_parser = parser.add_parser('question', help='Answer a question about the project')

        question_parser.add_argument('question', help='Question or goal to be answered')

        return question_parser

    def _execute_request_and_wait(self, request: SubmitLakeRequest):
        """
        Execute the request against OmniLake

        Keyword Arguments:
        request -- the request to execute
        """
        resp = self.omnilake.request(request)

        current_job_id = resp.response_body['job_id']

        current_job_type = resp.response_body['job_type']

        request_id = resp.response_body['lake_request_id']

        job_describe = DescribeJob(
            job_id=current_job_id,
            job_type=current_job_type,
        )

        job_resp = self.omnilake.request(job_describe)

        job_status = job_resp.response_body['status']

        while job_status != 'COMPLETED':
            time.sleep(10)

            job_resp = self.omnilake.request(job_describe)

            if job_resp.response_body['status'] != job_status:
                job_status = job_resp.response_body['status']

                if job_status == 'FAILED':
                    print(f'Job failed: {job_resp.response_body["status_message"]}')

                    return None

                print(f'Job status updated: {job_status}')

        print(f'Final job status: {job_status}')

        started = datetime.fromisoformat(job_resp.response_body['started'])

        ended = datetime.fromisoformat(job_resp.response_body['ended'])

        total_run_time = ended - started

        print(f'Total run time: {total_run_time}')

        return request_id

    def run(self, args):
        """
        Execute the command
        """
        starting_dir = args.base_dir

        archive_name = starting_dir.split('/')[-1]

        print(f'Requesting information against archive: {archive_name}')

        goal = f"Answer the following question: {args.question}"

        # Run an information request against OmniLake
        request = SubmitLakeRequest(
            lookup_instructions=[
                VectorLookup(
                    archive_id=archive_name,
                    max_entries=10,
                    query_string=args.question,
                ),
            ],
            processing_instructions=SummarizationProcessor(
                goal=goal,
                model_id="anthropic.claude-v2:1",
                include_source_metadata=True,
            ),
            response_config=DirectResponseConfig(),
        )

        request_id = self._execute_request_and_wait(request=request)

        if not request_id:
            print('Request failed to complete, check logs for more information')

            return

        request_request_obj = DescribeLakeRequest(lake_request_id=request_id)

        resp = self.omnilake.request(request_request_obj)

        entry_id = resp.response_body['response_entry_id']

        content_resp = self.omnilake.request(GetEntry(entry_id=entry_id))

        entry_content = content_resp.response_body['content']

        print(f"Response from server\n=================\n\n{entry_content}")