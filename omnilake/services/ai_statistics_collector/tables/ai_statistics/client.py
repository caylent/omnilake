from datetime import datetime, UTC as utc_tz
from typing import Optional
from uuid import uuid4

from da_vinci.core.orm.client import (
    TableClient,
    TableObject,
    TableObjectAttribute,
    TableObjectAttributeType,
    TableScanDefinition,
)


class InvocationStatistic(TableObject):
    table_name = "ai_statistics"

    description = "Tracks information about AI invocations that OmniLake has made."

    partition_key_attribute = TableObjectAttribute(
        name="invocation_id",
        attribute_type=TableObjectAttributeType.STRING,
        description="Unique identifier for the AI invocation.",
        default=lambda: str(uuid4()),
    )

    sort_key_attribute = TableObjectAttribute(
        name="job_type",
        attribute_type=TableObjectAttributeType.STRING,
        description="The type of job that invoked the AI.",
    )

    ttl_attribute = TableObjectAttribute(
        name="time_to_live",
        attribute_type=TableObjectAttributeType.DATETIME,
        description="The time-to-live for the AI invocation, used by DynamoDB TTL.",
    )

    attributes = [
        TableObjectAttribute(
            name="created_on",
            attribute_type=TableObjectAttributeType.DATETIME,
            description="The time the AI invocation was created.",
            default=lambda: datetime.now(tz=utc_tz),
        ),

        TableObjectAttribute(
            name="job_id",
            attribute_type=TableObjectAttributeType.STRING,
            description="Unique identifier for the job that invoked the AI.",
        ),

        TableObjectAttribute(
            name="model_id",
            attribute_type=TableObjectAttributeType.STRING,
            description="The model ID of the AI that was invoked.",
        ),

        TableObjectAttribute(
            name="model_parameters",
            attribute_type=TableObjectAttributeType.JSON_STRING,
            description="The parameters used to invoke the AI.",
            optional=True,
        ),

        TableObjectAttribute(
            name="resulting_entry_id",
            attribute_type=TableObjectAttributeType.STRING,
            description="The entry ID of the resulting AI invocation.",
            optional=True,
        ),

        TableObjectAttribute(
            name="total_input_tokens",
            attribute_type=TableObjectAttributeType.NUMBER,
            description="The total number of input tokens for the AI invocation.",
        ),

        TableObjectAttribute(
            name="total_output_tokens",
            attribute_type=TableObjectAttributeType.NUMBER,
            description="The total number of output tokens for the AI invocation.",
        ),
    ]


class AIStatisticsScanDefinition(TableScanDefinition):
    """
    Scan definition for AI statistics.
    """
    def __init__(self):
        super().__init__(table_object_class=InvocationStatistic)


class AIStatisticsClient(TableClient):
    def __init__(self, app_name: Optional[str] = None, deployment_id: Optional[str] = None):
        """
        Initialize the AIStatisticsClient.

        Keyword Arguments
        app_name -- The name of the application that is using the client. Optional.
        deployment_id -- The deployment ID of the application that is using the client. Optional.
        """

        super().__init__(
            default_object_class=InvocationStatistic,
            app_name=app_name,
            deployment_id=deployment_id,
        )

    def get(self, invocation_id: str, job_type: str) -> Optional[InvocationStatistic]:
        """
        Retrieve an AI invocation by its ID and job type.

        Arguments
        invocation_id -- The ID of the AI invocation.
        job_type -- The type of job that invoked the AI.

        Returns
        The AI invocation object if found, otherwise None.
        """

        return self.get_object(invocation_id, job_type)

    def put(self, invocation: InvocationStatistic) -> None:
        """
        Create or update an AI invocation.

        Arguments
        invocation -- The AI invocation object to create or update.
        """

        self.put_object(invocation)