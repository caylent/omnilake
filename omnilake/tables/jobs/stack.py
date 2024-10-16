from constructs import Construct

from aws_cdk import aws_dynamodb as cdk_dynamodb

from da_vinci_cdk.constructs.dynamodb import DynamoDBTable
from da_vinci_cdk.stack import Stack

from omnilake.tables.jobs.client import Job


class JobsTable(Stack):
    def __init__(self, app_name: str, deployment_id: str,
                 scope: Construct, stack_name: str):
        super().__init__(
            app_name=app_name,
            deployment_id=deployment_id,
            scope=scope,
            stack_name=stack_name
        )

        self.table = DynamoDBTable.from_orm_table_object(
            scope=self,
            table_object=Job,
        )

        self.table.table.add_global_secondary_index(
            index_name="job_id-index",
            partition_key=cdk_dynamodb.Attribute(
                name='JobId',
                type=cdk_dynamodb.AttributeType.STRING
            )
        )