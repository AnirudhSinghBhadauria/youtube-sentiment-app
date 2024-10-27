import re  
import sys
import boto3
from awsglue.job import Job
from datetime import datetime
from awsglue.transforms import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'bucket_name',
    'prefix_path',
    'cockroach_url',
    'cockroach_user',
    'cockroach_password',
    'cockroach_schema'
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

bucket_name = args['bucket_name']
prefix_path = args['prefix_path']

cockroach_properties = {
    "driver": "org.postgresql.Driver",
    "url": args['cockroach_url'],
    "user": args['cockroach_user'],
    "password": args['cockroach_password'],
    "schema": args['cockroach_schema'],
    "ssl": "true",
    "sslmode": "require"  
}

def sanitize_table_name(name):
    name = re.sub(r'_+', '_', name) 
    name = re.sub(r'[^\w]', '', name)  
    if name[0].isdigit() or name[0] == '_':
        name = 't_' + name 
    
    return name[:1024] 

def get_table_name_from_path(file_path):
    parts = file_path.split('/')
    channel_name = parts[1]  
    file_name = parts[2].replace('.parquet', '')  
    video_id_and_date = '_'.join(file_name.split('_')[1:]) 
    table_name = f"{channel_name}_{video_id_and_date}"
    sanitized_name = sanitize_table_name(table_name)  
    return sanitized_name

def write_to_cockroachdb(df, table_name):
    df.write \
        .format("jdbc") \
        .option("driver", cockroach_properties["driver"]) \
        .option("url", cockroach_properties["url"]) \
        .option("user", cockroach_properties["user"]) \
        .option("password", cockroach_properties["password"]) \
        .option("dbtable", f"{cockroach_properties['schema']}.{table_name}") \
        .option("ssl", cockroach_properties["ssl"]) \
        .option("sslmode", cockroach_properties["sslmode"]) \
        .mode("append") \
        .save()

def list_parquet_files():
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    
    parquet_files = []
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix_path):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.parquet'):
                    parquet_files.append(obj['Key'])
    
    return parquet_files

def log_processing(file_path, status, error=None):
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    log_df = spark.createDataFrame([
        (file_path, status, timestamp, error)
    ], ["file_path", "status", "processed_at", "error_message"])
    
    write_to_cockroachdb(log_df, "sentiment_processing_log")

try:
    parquet_files = list_parquet_files()
    print(f"Found {len(parquet_files)} parquet files to process")
    
    for file_path in parquet_files:
        try:
            print(f"Processing file: {file_path}")
            
            s3_full_path = f"s3://{bucket_name}/{file_path}"
            df = spark.read.parquet(s3_full_path)
            
            table_name = get_table_name_from_path(file_path)
            print(f"Creating/updating table: {table_name}")
            
            df = df.withColumnRenamed("updated_at", "comment_time") \
                   .withColumn("processed_at", lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            
            write_to_cockroachdb(df, table_name)
            
            log_processing(file_path, "SUCCESS")
            print(f"Successfully processed: {file_path}")
            
        except Exception as e:
            error_msg = str(e)
            print(f"Error processing file {file_path}: {error_msg}")
            log_processing(file_path, "FAILED", error_msg)
            continue

except Exception as e:
    print(f"Fatal error occurred: {str(e)}")
    raise e

finally:
    job.commit()
