import boto3
import json
from botocore.exceptions import ClientError

s3 = boto3.client('s3')

def handler(event, context):
    config = event['configuration']
    bucket_name = config['bucket']

    try:
        # Check if bucket already exists
        s3.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' already exists.")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": f"Bucket '{bucket_name}' already exists.",
                "bucket": bucket_name
            })
        }

    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == '404':
            # Bucket does not exist, create it
            try:
                s3.create_bucket(Bucket=bucket_name)
                print(f"✅ Created bucket '{bucket_name}' successfully.")

                return {
                    "statusCode": 201,
                    "body": json.dumps({
                        "message": f"Bucket '{bucket_name}' created successfully.",
                        "bucket": bucket_name
                    })
                }

            except Exception as creation_error:
                error_msg = f"❌ Error creating bucket '{bucket_name}': {str(creation_error)}"
                print(error_msg)

                return {
                    "statusCode": 500,
                    "body": json.dumps({"error": error_msg})
                }
        else:
            # Other errors (e.g., permissions)
            error_msg = f"❌ Error accessing bucket '{bucket_name}': {str(e)}"
            print(error_msg)

            return {
                "statusCode": 500,
                "body": json.dumps({"error": error_msg})
            }
