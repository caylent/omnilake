import logging
import uuid

import boto3

from botocore.exceptions import ClientError

# Using this to grab the app_name and deployment_id
from da_vinci.core.execution_environment import load_runtime_environment_variables


class SSMSecretManager:
    def __init__(self):
        """
        Initialize the SSM Secret Manager
        
        Args:
            prefix: The SSM parameter path prefix to use
            region: AWS region, if None uses default from boto3
        """
        self.ssm_client = boto3.client("ssm")

        # Grab deployment id from environment variables
        env_vars = load_runtime_environment_variables()

        app_name = env_vars['app_name']

        deployment_id = env_vars['deployment_id']

        self.prefix = f"/{app_name}/{deployment_id}/omnilake_secrets"

        logging.debug(f"SSM Secret Manager initialized with prefix: {self.prefix}")

    def mask_secret(self, secret_value: str) -> str:
        """
        Store a secret in SSM and return a random ID to reference it
        
        Args:
            secret_value: The secret value to store
            
        Returns:
            str: Random ID that can be used to retrieve the secret
        """
        # Generate random ID
        secret_id = str(uuid.uuid4())

        param_path = f"{self.prefix}/{secret_id}"
        
        # Store in SSM with encryption
        self.ssm_client.put_parameter(
            Name=param_path,
            Value=secret_value,
            Type='SecureString',
            Overwrite=False
        )

        return f"SECRET:{secret_id}"

    def unmask_secret(self, secret_id: str) -> str:
        """
        Retrieve a secret from SSM using its ID
        
        Args:
            secret_id: The ID returned from mask_secret()
            
        Returns:
            str: The original secret value
            
        Raises:
            ValueError: If the secret doesn't exist
        """
        id_only = secret_id.split(":")[1]

        param_path = f"{self.prefix}{id_only}"
        
        try:
            response = self.ssm_client.get_parameter(
                Name=param_path,
                WithDecryption=True
            )

            return response['Parameter']['Value']
        except ClientError as e:
            if e.response['Error']['Code'] == 'ParameterNotFound':
                raise ValueError(f"No secret found for ID: {secret_id}") from e

            raise

    def delete_secret(self, secret_id: str) -> None:
        """
        Delete a secret from SSM
        
        Args:
            secret_id: The ID of the secret to delete
            
        Raises:
            ValueError: If the secret doesn't exist
        """
        param_path = f"{self.prefix}{secret_id}"
        
        try:
            self.ssm_client.delete_parameter(Name=param_path)

        except ClientError as e:
            if e.response['Error']['Code'] == 'ParameterNotFound':
                raise ValueError(f"No secret found for ID: {secret_id}") from e

            raise