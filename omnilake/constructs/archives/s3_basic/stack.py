from aws_cdk import (
    aws_s3 as s3,
    aws_iam as iam,
    Duration
)
from constructs import Construct
from da_vinci_cdk.constructs.service import SimpleRESTService

class S3BasicArchiveStack(SimpleRESTService):
    def __init__(self, scope: Construct, app_name: str, app_base_image: str, **kwargs):
        super().__init__(
            scope=scope,
            base_image=app_base_image,
            entry="runtime",
            handler="index.handler",  # Entry point is your index.py handler
            timeout=Duration.minutes(5),
            service_name='s3-basic-archive',
            environment={
                'ARCHIVE_BUCKET_NAME': f"{app_name}-s3-basic-archive"
            },
            **kwargs
        )

        # Create an S3 bucket (or reference an existing one)
        archive_bucket = s3.Bucket(
            self,
            'S3BasicArchiveBucket',
            bucket_name=f"{app_name}-s3-basic-archive"
        )

        # Grant Lambda necessary permissions on the bucket
        archive_bucket.grant_read_write(self.service_function)

        # Explicit IAM policy (if additional granular permissions are needed)
        self.service_function.add_to_role_policy(iam.PolicyStatement(
            actions=[
                's3:PutObject',
                's3:GetObject',
                's3:ListBucket'
            ],
            resources=[
                archive_bucket.bucket_arn,
                f"{archive_bucket.bucket_arn}/*"
            ]
        ))
