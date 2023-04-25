python
import json
import requests
import boto3

s3 = boto3.client('s3')

def lambda_handler(event, context):
    url = event['url']
    response = requests.get(url)
    data = response.text
    key = event['key']
    bucket_name = event['bucket_name']
    s3.put_object(Bucket=bucket_name, Key=key, Body=data)
    return {
        'statusCode': 200,
        'body': json.dumps('Data scraped and uploaded to S3!')
    }
