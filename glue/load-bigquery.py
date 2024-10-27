import re
import os
import sys
import boto3
import pandas as pd
from awsglue.job import Job
from datetime import datetime
from google.cloud import bigquery
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions


args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'bucket_name',
    's3_key_path',
    'prefix_path',
    'bq_project',
    'bq_dataset'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def sanitize_table_name(name):
    name = re.sub(r'_+', '_', name)
    name = re.sub(r'[^\w]', '', name)
    if name[0].isdigit() or name[0] == '_':
        name = 't_' + name
    
    return name[:1024]

def get_table_name_from_path(file_path):
    try:
        parts = file_path.split('/')
        if len(parts) < 3:
            raise ValueError(f"Invalid file path format: {file_path}")
        
        channel_name = parts[1]
        file_name = parts[2].replace('.parquet', '')
        
        video_id_and_date = '_'.join(file_name.split('_')[1:])
        
        table_name = f"{channel_name}_{video_id_and_date}"
        sanitized_name = sanitize_table_name(table_name)
        
        print(f"Original table name: {table_name}")
        print(f"Sanitized table name: {sanitized_name}")
        
        return sanitized_name
        
    except Exception as e:
        raise ValueError(f"Error processing file path {file_path}: {str(e)}")

def write_to_bigquery(df, table_name):
    try:
        print(f"Converting Spark DataFrame to Pandas (rows: {df.count()})")
        pandas_df = df.toPandas()
        
        if pandas_df.empty:
            raise ValueError("DataFrame is empty")
        
        dataset_name = args['bq_dataset'].split('.')[-1]  # Takes 'sentiments' from 'youtube-sentiment-439915.sentiments'
        table_id = f"{args['bq_project']}.{dataset_name}.{table_name}"
        print(f"Writing to BigQuery table: {table_id}")
        
        pandas_df.to_gbq(
            destination_table=f"{dataset_name}.{table_name}",
            project_id=args['bq_project'],
            if_exists='append',
            progress_bar=False
        )
        
        bq_client = bigquery.Client()
        query = f"SELECT COUNT(*) as count FROM `{table_id}`"
        result = bq_client.query(query).result()
        row_count = list(result)[0].count
        print(f"Verified {row_count} rows in BigQuery table {table_id}")
        
    except Exception as e:
        raise Exception(f"Error writing to BigQuery: {str(e)}")

def list_parquet_files():
    try:
        s3_client = boto3.client('s3')
        paginator = s3_client.get_paginator('list_objects_v2')
        
        parquet_files = []
        for page in paginator.paginate(Bucket=args['bucket_name'], Prefix=args['prefix_path']):
            if 'Contents' in page:
                for obj in page['Contents']:
                    if obj['Key'].endswith('.parquet'):
                        parquet_files.append(obj['Key'])
        
        if not parquet_files:
            print(f"No parquet files found in s3://{args['bucket_name']}/{args['prefix_path']}")
        
        return parquet_files
    except Exception as e:
        raise Exception(f"Error listing parquet files: {str(e)}")

try:
    key_file_path = "/tmp/your-key.json"
    s3_client = boto3.client('s3')
    print(f"Downloading service account key from s3://{args['bucket_name']}/{args['s3_key_path']}")
    s3_client.download_file(args['bucket_name'], args['s3_key_path'], key_file_path)
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_file_path
    
    bq_client = bigquery.Client()
    dataset_name = args['bq_dataset'].split('.')[-1]
    print(f"Testing connection to dataset: {args['bq_project']}.{dataset_name}")
    try:
        bq_client.get_dataset(f"{args['bq_project']}.{dataset_name}")
        print("Successfully connected to BigQuery and verified dataset exists")
    except Exception as e:
        raise Exception(f"Failed to connect to BigQuery or dataset not found: {str(e)}")
    
    parquet_files = list_parquet_files()
    print(f"Found {len(parquet_files)} parquet files to process")
    
    for file_path in parquet_files:
        try:
            print(f"\nProcessing file: {file_path}")
            
            s3_full_path = f"s3://{args['bucket_name']}/{file_path}"
            df = spark.read.parquet(s3_full_path)
            print(f"Read parquet file with {df.count()} rows")
            
            df = df.withColumnRenamed("updated_at", "comment_time") \
                  .drop("source_file") \
                  .withColumn("processed_at", lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            
            table_name = get_table_name_from_path(file_path)
            print(f"Creating/updating table: {table_name}")
            
            write_to_bigquery(df, table_name)
            print(f"Successfully processed: {file_path}")
            
        except Exception as e:
            print(f"Error processing file {file_path}: {str(e)}")
            raise  
            
except Exception as e:
    print(f"Fatal error occurred: {str(e)}")
    raise e

finally:
    if os.path.exists(key_file_path):
        os.remove(key_file_path)
    job.commit()
    