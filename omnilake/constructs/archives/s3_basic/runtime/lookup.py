import boto3
import json
import urllib.parse

s3_client = boto3.client('s3')

def handler(event, context):
    config = event['configuration']
    bucket_name = config['bucket']
    prefix = config.get('prefix', '')
    object_key = event['lookup_key']

    # Construct full key with optional prefix
    full_key = f"{prefix.rstrip('/')}/{object_key}" if prefix else object_key
    full_key = urllib.parse.unquote_plus(full_key)

    try:
        # Retrieve object content from S3
        response = s3_client.get_object(Bucket=bucket_name, Key=full_key)
        file_content = response['Body'].read().decode('utf-8')

        print(f"✅ Successfully retrieved '{full_key}'.")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "key": full_key,
                "content": file_content
            })
        }

    except Exception as e:
        error_message = f"❌ Error retrieving '{full_key}': {str(e)}"
        print(error_message)

        return {
            "statusCode": 500,
            "body": json.dumps({"error": error_message})
        }
