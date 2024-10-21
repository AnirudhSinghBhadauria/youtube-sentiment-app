from aws_cdk import App, Stack
from constructs import Construct
from aws_cdk import aws_lambda as _lambda
from aws_cdk import aws_s3 as s3
from aws_cdk import aws_iam as iam
import os
from dotenv import load_dotenv

load_dotenv()

class YoutubeSentimentFinalStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        bucket = s3.Bucket(self, "YoutubeSentimentBucket")

        docker_lambda = _lambda.DockerImageFunction(
            self, 
            "YoutubeSentimentLambda",
            code=_lambda.DockerImageCode.from_image_asset(
                directory="./lambda",
            ),
            environment={
                "DEVELOPER_KEY": os.getenv("DEVELOPER_KEY"),
                "S3_BUCKET_NAME": bucket.bucket_name,
            },
        )

        bucket.grant_write(docker_lambda)

app = App()
YoutubeSentimentFinalStack(app, "YoutubeSentimentFinalStack")
app.synth()
