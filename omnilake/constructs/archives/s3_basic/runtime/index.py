import boto3
import json
import urllib.parse
from omnilake.client.client import OmniLake
from omnilake.client.request_definitions import AddEntry, AddSource, CreateSourceType
from omnilake.client.exceptions import OmniLakeClientError

s3_client = boto3.client('s3')
omnilake = OmniLake()

def ensure_source_type_exists():
    try:
        source_type_req = CreateSourceType(
            name='s3_document',
            description='Documents indexed from S3 storage',
            required_fields=['bucket', 'key']
        )
        omnilake.create_source_type(source_type_req)
        print("✅ Source type 's3_document' created successfully.")
    except OmniLakeClientError as e:
        if e.status_code == 409:
            print("ℹ️ Source type 's3_document' already exists.")
        else:
            raise

def handler(event, context):
    config = event['configuration']
    bucket_name = config['bucket']
    prefix = config.get('prefix', '')
    object_key = event['index_key']

    # Ensure 's3_document' source type exists (runs once)
    ensure_source_type_exists()

    # Prepend prefix if provided
    full_key = f"{prefix.rstrip('/')}/{object_key}" if prefix else object_key
    full_key = urllib.parse.unquote_plus(full_key)

    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=full_key)
        file_content = response['Body'].read().decode('utf-8')

        source_result = omnilake.add_source(AddSource(
            source_type='s3_document',
            source_arguments={
                'bucket': bucket_name,
                'key': full_key
            }
        ))

        source_rn = source_result.response_body['resource_name']

        add_entry_result = omnilake.add_entry(AddEntry(
            content=file_content,
            sources=[source_rn]
        ))

        entry_id = add_entry_result.response_body['entry_id']

        print(f"✅ Indexed '{full_key}' successfully as entry ID {entry_id}")

        return {
            "statusCode": 200,
            "body": json.dumps({"entry_id": entry_id})
        }

    except Exception as e:
        error_message = f"❌ Failed to index '{full_key}': {str(e)}"
        print(error_message)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": error_message})
        }
