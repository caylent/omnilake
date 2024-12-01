from constructs import Construct

from aws_cdk import aws_dynamodb as cdk_dynamodb

from da_vinci_cdk.constructs.dynamodb import DynamoDBTable
from da_vinci_cdk.stack import Stack

from omnilake.tables.sources.client import Source


class SourcesTable(Stack):
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
            table_object=Source,
        )

        self.table.table.add_global_secondary_index(
            index_name="attribute-key-index",
            partition_key=cdk_dynamodb.Attribute(
                name='AttributeKey',
                type=cdk_dynamodb.AttributeType.STRING
            ),
        )