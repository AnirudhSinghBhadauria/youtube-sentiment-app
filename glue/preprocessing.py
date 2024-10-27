import sys
import re
import boto3
import emoji
from awsglue.job import Job
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from urllib.parse import urlparse

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

def preprocess_youtube_comments(text):
    if not isinstance(text, str):
        return ''
    text = text.lower()
    text = emoji.demojize(text)
    text = re.sub(r'https?://\S+|www\.\S+', '', text, flags=re.MULTILINE)
    text = re.sub(r'@\w+', '', text)
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'(.)\1{2,}', r'\1\1', text)
    text = re.sub(r'[^a-z0-9\s.,!?\U0001F300-\U0001F9FF\U0001FA70-\U0001FAFF]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def clean_comments(df):
    df = df.dropDuplicates()
    df = df.na.drop()
    preprocess_udf = udf(preprocess_youtube_comments, StringType())
    df = df.withColumn("text", preprocess_udf("text"))
    df = df.filter(df.text != "")
    df = df.na.drop(subset=["text"])
    return df

source_bucket = "youtube-comments-analyzer"
source_prefix = "stage"
target_prefix = "curated"
s3_client = boto3.client('s3')

def list_all_files():
    all_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    try:
        for page in paginator.paginate(Bucket=source_bucket, Prefix=source_prefix):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.parquet'):
                        all_files.append(obj['Key'])
    except Exception as e:
        print(f"Error listing files: {str(e)}")
        raise
    return all_files

all_files = list_all_files()

for file_path in all_files:
    try:
        source_file_path = f"s3://{source_bucket}/{file_path}"
        
        target_file_path = file_path.replace(source_prefix, target_prefix)
        target_path = f"s3://{source_bucket}/{target_file_path}"
        
        temp_target_path = target_path.replace('.parquet', '')
        
        print(f"Processing file: {source_file_path}")
        print(f"Target path: {target_path}")
        
        dynamic_frame = glueContext.create_dynamic_frame.from_options(
            connection_type="s3",
            connection_options={"paths": [source_file_path]},
            format="parquet"
        )
        
        df = dynamic_frame.toDF()
        processed_df = clean_comments(df)
        
        processed_df.coalesce(1).write.mode('overwrite').parquet(temp_target_path)
        
        temp_prefix = target_file_path.replace('.parquet', '')
        found_part_file = False
        
        for obj in s3_client.list_objects_v2(Bucket=source_bucket, Prefix=temp_prefix)['Contents']:
            if obj['Key'].endswith('.parquet'):
                copy_source = {'Bucket': source_bucket, 'Key': obj['Key']}
                s3_client.copy_object(
                    CopySource=copy_source,
                    Bucket=source_bucket,
                    Key=target_file_path
                )

                s3_client.delete_object(Bucket=source_bucket, Key=obj['Key'])
                found_part_file = True
                break
        
        if not found_part_file:
            raise Exception("Could not find processed parquet file")
            
        print(f"Successfully processed and saved file to: {target_path}")
        
        s3_client.delete_object(Bucket=source_bucket, Key=file_path)
        print(f"Deleted original file: {file_path}")
        
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        continue

job.commit()