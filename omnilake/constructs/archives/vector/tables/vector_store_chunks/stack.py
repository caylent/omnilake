from constructs import Construct

from aws_cdk import aws_dynamodb as cdk_dynamodb

from da_vinci_cdk.constructs.dynamodb import DynamoDBTable
from da_vinci_cdk.stack import Stack

from omnilake.constructs.archives.vector.tables.vector_store_chunks.client import VectorStoreChunk


class VectorStoreChunksTable(Stack):
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
            table_object=VectorStoreChunk,
        )

        self.table.table.add_global_secondary_index(
            index_name="archive_entry-index",
            partition_key=cdk_dynamodb.Attribute(
                name='ArchiveId',
                type=cdk_dynamodb.AttributeType.STRING
            ),
            sort_key=cdk_dynamodb.Attribute(
                name='EntryId',
                type=cdk_dynamodb.AttributeType.STRING
            ),
        )

        self.table.table.add_global_secondary_index(
            index_name="vector_id-index",
            partition_key=cdk_dynamodb.Attribute(
                name='ArchiveId',
                type=cdk_dynamodb.AttributeType.STRING
            ),
            sort_key=cdk_dynamodb.Attribute(
                name='VectorStoreId',
                type=cdk_dynamodb.AttributeType.STRING
            ),
        )