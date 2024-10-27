import boto3

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    bucket_name = 'youtube-comments-analyzer'
    source_prefix = 'processed/'
    destination_prefix = 'dump/'

    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=source_prefix)

    if 'Contents' in response:
        for obj in response['Contents']:
            source_key = obj['Key']  
            destination_key = source_key.replace(source_prefix, destination_prefix, 1)

            s3.copy_object(
                Bucket=bucket_name,
                CopySource={'Bucket': bucket_name, 'Key': source_key},
                Key=destination_key
            )

            s3.delete_object(Bucket=bucket_name, Key=source_key)

        s3.put_object(Bucket=bucket_name, Key=f'{source_prefix}.keep', Body='')

    return {'status': 'success'}
