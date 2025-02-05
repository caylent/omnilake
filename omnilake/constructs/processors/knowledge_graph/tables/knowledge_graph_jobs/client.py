'''Knowledge Graph Job Processor Table Client'''

from datetime import datetime, UTC as utc_tz
from typing import Dict, Optional, Set
from uuid import uuid4

from da_vinci.core.orm import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
)


class KnowledgeGraphJob(TableObject):
    table_name = 'knowledge_graph_jobs'

    description = 'Table tracking all knowledge graph processor jobs'

    partition_key_attribute = TableObjectAttribute(
        name="knowledge_graph_processing_id",
        attribute_type=TableObjectAttributeType.STRING,
        description="The knowledge graph processing id",
        default=lambda: str(uuid4()),
    )

    attributes = [
        TableObjectAttribute(
            name="ai_invocation_ids",
            attribute_type=TableObjectAttributeType.STRING_SET,
            description="The AI invocation ids associated with the knowledge graph job.",
            optional=True,
            default=lambda: set(),
        ),

        TableObjectAttribute(
            name="configuration",
            attribute_type=TableObjectAttributeType.JSON_STRING,
            description="The configuration of the knowledge graph job.",
        ),

        TableObjectAttribute(
            name="created_on",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The date and time the summarization job context was created.",
            default=lambda: datetime.now(tz=utc_tz),
        ),

        TableObjectAttribute(
            name="extracted_entry_ids",
            attribute_type=TableObjectAttributeType.STRING_SET,
            description="The extracted entry ids.",
            default=lambda: set(),
        ),

        TableObjectAttribute(
            name="goal",
            attribute_type=TableObjectAttributeType.STRING,
            description="The goal of the summarization job context.",
        ),

        TableObjectAttribute(
            name="filtered_entry_ids",
            attribute_type=TableObjectAttributeType.STRING_SET,
            description="The selection entry ids.",
            default=lambda: set(),
        ),

        TableObjectAttribute(
            name="lake_request_id",
            attribute_type=TableObjectAttributeType.STRING,
            description="The lake request ID of the summarization job context.",
        ),

        TableObjectAttribute(
            name="parent_job_id",
            attribute_type=TableObjectAttributeType.STRING,
            description="The parent job ID of the summarization job context.",
        ),

        TableObjectAttribute(
            name="parent_job_type",
            attribute_type=TableObjectAttributeType.STRING,
            description="The parent job type of the summarization job context.",
        ),

        TableObjectAttribute(
            name="remaining_processes",
            attribute_type=TableObjectAttributeType.NUMBER,
            description="The current number of remaining processes running for the job.",
            default=0,
        ),

        TableObjectAttribute(
            name="stage",
            attribute_type=TableObjectAttributeType.STRING,
            description="The current stage of the knowledge graph job.",
            default="EXTRACTION"
        ),
    ]

    def __init__(self, goal: str, lake_request_id: str, parent_job_id: str, parent_job_type: str,
                 ai_invocation_ids: Optional[Set[str]] = None, configuration: Optional[Dict] = None,
                 created_on: Optional[datetime] = None, extracted_entry_ids: Optional[Set] = None,
                 filtered_entry_ids: Optional[Set] = None, knowledge_graph_processing_id: Optional[str] = None,
                 remaining_processes: Optional[int] = None, stage: Optional[str] = None):
        """
        Initialize a knowledge graph job.

        Keyword Arguments:
        ai_invocation_ids -- The AI invocation ids associated with the knowledge graph job.
        configuration -- The configuration of the knowledge graph job.
        created_on -- The date and time the summarization job context was created.
        extracted_entry_ids -- The extracted entry ids.
        goal -- The goal of the summarization job context.
        filtered_entry_ids -- The filtered entry ids.
        lake_request_id -- The lake request ID of the summarization job context.
        knowledge_graph_processing_id -- The knowledge graph processing id.
        parent_job_id -- The parent job ID of the summarization job context.
        parent_job_type -- The parent job type of the summarization job context.
        remaining_processes -- The current number of remaining extractions running.
        selection_entry_ids -- The selection entry ids.
        stage -- The current stage of the knowledge graph job.
        """

        super().__init__(
            ai_invocation_ids=ai_invocation_ids,
            configuration=configuration,
            created_on=created_on,
            extracted_entry_ids=extracted_entry_ids,
            goal=goal,
            lake_request_id=lake_request_id,
            knowledge_graph_processing_id=knowledge_graph_processing_id,
            parent_job_id=parent_job_id,
            parent_job_type=parent_job_type,
            remaining_processes=remaining_processes,
            filtered_entry_ids=filtered_entry_ids,
            stage=stage,
        )


class KnowledgeGraphJobClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        super().__init__(
            app_name=app_name,
            deployment_id=deployment_id,
            default_object_class=KnowledgeGraphJob,
        )

    def add_completed_entry(self, entry_id: str, knowledge_graph_processing_id: str, ai_invocation_id: Optional[str] = None, 
                            decrement_process: Optional[bool] = True, entry_attr_name: str = 'ExtractedEntryIds') -> int:
        """
        Add a completed entry to the knowledge graph job.

        Keyword arguments:
        entry_id -- The entry ID of the resource to add
        knowledge_graph_processing_id -- The knowledge graph processing ID
        ai_invocation_id -- The AI invocation ID of the resource to add
        decrement_process -- Whether or not to decrement the remaining processes (default: {True})
        """
        update_expression = f"ADD {entry_attr_name} :entry_id_set"

        expression_attribute_values = {
            ':entry_id_set': {'SS': [entry_id]}
        }

        if ai_invocation_id:
            update_expression += ", AiInvocationIds :ai_invocation_id_set"

            expression_attribute_values.update({
                ':ai_invocation_id_set': {'SS': [ai_invocation_id]}
            })

        if decrement_process:
            update_expression += " SET RemainingProcesses = if_not_exists(RemainingProcesses, :start) - :decrement"

            expression_attribute_values.update({
                ':decrement': {'N': "1"},
                ':start': {'N': "0"},
            })

        response = self.client.update_item(
            TableName=self.table_endpoint_name,
            Key={
                'KnowledgeGraphProcessingId': {'S': knowledge_graph_processing_id},
            },
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_attribute_values,
            ReturnValues='UPDATED_NEW',
        )

        remaining_process = int(response['Attributes']['RemainingProcesses']['N'])
    
        return remaining_process

    def get(self, knowledge_graph_request_id: str, consistent_read: bool = False) -> Optional[KnowledgeGraphJob]:
        """
        Get a knowledge graph job by its knowledge_graph_request_id.

        Keyword Arguments:
        knowledge_graph_request_id -- The knowledge graph request id.
        """
        return self.get_object(partition_key_value=knowledge_graph_request_id, consistent_read=consistent_read)

    def put(self, knowledge_graph_job: KnowledgeGraphJob) -> None:
        """
        Put a knowledge graph job.

        Keyword Arguments:
        knowledge_graph -- The knowledge graph job.
        """
        return self.put_object(table_object=knowledge_graph_job)