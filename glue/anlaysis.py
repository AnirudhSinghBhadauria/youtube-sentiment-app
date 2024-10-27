import re
import sys
import boto3
import numpy as np
import pandas as pd
from awsglue.job import Job
from awsglue.transforms import *
from scipy.special import softmax
from transformers import AutoTokenizer
from awsglue.context import GlueContext
from pyspark.context import SparkContext
from awsglue.utils import getResolvedOptions
from transformers import AutoModelForSequenceClassification

BUCKET_NAME = 'youtube-comments-analyzer'  
MODEL = "cardiffnlp/twitter-roberta-base-sentiment"

def get_s3_paths(prefix='curated/'):
    s3_client = boto3.client('s3')
    paginator = s3_client.get_paginator('list_objects_v2')
    
    files = []
    for page in paginator.paginate(Bucket=BUCKET_NAME, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                if obj['Key'].endswith('.parquet'):
                    files.append(f"s3://{BUCKET_NAME}/{obj['Key']}")
    return files

def delete_source_file(s3_path):
    s3_client = boto3.client('s3')
    key = s3_path.replace(f"s3://{BUCKET_NAME}/", "")
    try:
        s3_client.delete_object(Bucket=BUCKET_NAME, Key=key)
        print(f"Deleted source file: {s3_path}")
        return True
    except Exception as e:
        print(f"Error deleting {s3_path}: {str(e)}")
        return False

def process_comments(input_path):
    try:
        tokenizer = AutoTokenizer.from_pretrained(MODEL)
        model = AutoModelForSequenceClassification.from_pretrained(MODEL)
        
        comment_df = pd.read_parquet(input_path)
        
        polarity = []
        for index, comment in comment_df.iterrows():
            encoded_text = tokenizer(comment['text'], return_tensors='pt', max_length=512, truncation=True)
            output = model(**encoded_text)
            scores = output[0][0].detach().numpy()
            scores = softmax(scores)
            comment_df.at[index, 'negative'] = np.round(scores[0] * 100, 3)
            comment_df.at[index, 'neutral'] = np.round(scores[1] * 100, 3)
            comment_df.at[index, 'positive'] = np.round(scores[2] * 100, 3)
        
        for index, row in comment_df.iterrows():
            max_index = np.argmax([row['negative'], row['neutral'], row['positive']])
            polarity.append(max_index)
        
        comment_df['polarity'] = polarity
        comment_df['polarity'] = comment_df['polarity'].map({0: 'negative', 1: 'neutral', 2: 'positive'})
        
        output_path = input_path.replace('curated/', 'processed/')
        
        comment_df.to_parquet(output_path)
        
        if delete_source_file(input_path):
            return output_path
        else:
            raise Exception("Failed to delete source file")
            
    except Exception as e:
        print(f"Error processing {input_path}: {str(e)}")
        return None

def main():
    sc = SparkContext()
    glueContext = GlueContext(sc)
    job = Job(glueContext)
    job.init('sentiment_analysis_job', args={})
    
    input_files = get_s3_paths()
    
    print(f"Processing files from bucket: {BUCKET_NAME}")
    print(f"Found {len(input_files)} files to process")
    
    successful_processes = 0
    failed_processes = 0
    
    for input_path in input_files:
        try:
            output_path = process_comments(input_path)
            if output_path:
                print(f"Successfully processed {input_path} to {output_path}")
                successful_processes += 1
            else:
                print(f"Failed to process {input_path}")
                failed_processes += 1
        except Exception as e:
            print(f"Error processing {input_path}: {str(e)}")
            failed_processes += 1
    
    print(f"\nProcessing Summary:")
    print(f"Total files processed: {len(input_files)}")
    print(f"Successful: {successful_processes}")
    print(f"Failed: {failed_processes}")
    
    job.commit()

if __name__ == '__main__':
    main()