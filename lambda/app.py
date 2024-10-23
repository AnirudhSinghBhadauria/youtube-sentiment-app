import io
import os
import json
import boto3
import pandas as pd
import botocore.exceptions
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError


DEVELOPER_KEY = os.getenv('DEVELOPER_KEY')
youtube = build("youtube", "v3", developerKey=DEVELOPER_KEY)

s3 = boto3.client('s3')

def generate_timestamp(timestamp):
    timestamp_object = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ")
    return timestamp_object.strftime("%Y%m%d%H%M%S")

def format_duration(fetched_duration):
    return int(''.join(char for char in fetched_duration if char.isnumeric()))

def get_video_duration(video_id):
    video_response = youtube.videos().list(part="contentDetails", id=video_id).execute()
    video_duration = video_response['items'][0]['contentDetails']['duration']
    return format_duration(video_duration)

def get_channel_handle(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    return response['items'][0]['snippet']['customUrl'][1:]

def get_comments(video_id, video_title, video_release, channel_handle):
    comments = []
    next_page_token = None

    while True:
        request = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100,
            pageToken=next_page_token
        )
        response = request.execute()

        for item in response['items']:
            comment = item['snippet']['topLevelComment']['snippet']
            comments.append([
                comment['authorDisplayName'],
                comment['publishedAt'],
                comment['likeCount'],
                comment['textOriginal'],
            ])

        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break

    video_name = "".join([char for char in video_title[:16] if char.isalnum()])
    df = pd.DataFrame(comments, columns=['author', 'updated_at', 'like_count', 'text'])

    filename = f"stage/{channel_handle}/{video_name}_{video_id}_{generate_timestamp(video_release)}.parquet"

    # Check if the Parquet file already exists in the S3 bucket
    bucket_name = os.getenv('S3_BUCKET_NAME')
    try:
        s3.head_object(Bucket=bucket_name, Key=filename)
        print(f"Skipping file {filename} as it already exists in the S3 bucket.")
        return None
    except botocore.exceptions.ClientError as err:
        if err.response['Error']['Code'] == '404':
            pass
        else:
            raise err

    parquet_buffer = io.BytesIO()
    df.to_parquet(parquet_buffer)
    parquet_buffer.seek(0)

    s3.put_object(Bucket=bucket_name, Key=filename, Body=parquet_buffer.getvalue())

    return filename

def get_channel_videos(channel_id, max_videos=2):
    videos = []
    channel_response = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    ).execute()
    
    uploads_playlist_id = channel_response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]   
    next_page_token = None
    
    while True:
        playlist_items = youtube.playlistItems().list(
            part="snippet",
            playlistId=uploads_playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
            
        for item in playlist_items["items"]:
            video_id = item["snippet"]["resourceId"]["videoId"]
            video_title = item["snippet"]["title"]
            published_at = item["snippet"]["publishedAt"]
            
            if get_video_duration(video_id) > 300:
                videos.append({
                    "id": video_id, 
                    "title": video_title, 
                    "release": published_at
                })

            if len(videos) == max_videos:
                return videos

        next_page_token = playlist_items.get("nextPageToken")    
        if not next_page_token:
            break
    
    return videos

def lambda_handler(event, context):
    try:
        channel_ids = event['channel_ids']        
        
        processed_files = []
        skipped_files = [] 
               
        for channel_id in channel_ids:
            videos = get_channel_videos(channel_id)
            channel_handle = get_channel_handle(channel_id)            
            
            for video in videos:
                filename = get_comments(video['id'], video['title'], video['release'], channel_handle)
                if filename:
                    processed_files.append(filename)
                else:
                    video_title = "".join([char for char in video['title'][:16] if char.isalnum()])
                    skipped_files.append(f"stage/{channel_handle}/{video_title}_{video['id']}_{generate_timestamp(video['release'])}.parquet")     
                       
        return {
            'statusCode': 200,
            'body': json.dumps({
                'processed_files': processed_files,
                'skipped_files': skipped_files
            })
        }
    except Exception as err:
        print(f"Error: {str(err)}")
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error occurred: {str(err)}')
        }     
