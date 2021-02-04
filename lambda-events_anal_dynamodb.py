import json
import hashlib
import boto3
from datetime import datetime, timedelta
from pprint import pprint

def lambda_handler(event, context):

    bucket = 'events-anal-files-gz'
    file_name = event['Records'][0]['s3']['object']['key']
    yesterday = (datetime.strptime(file_name.split('.')[0], '%d-%m-%Y') - timedelta(days=1)).strftime('%d-%m-%Y')
    table_name = 'events_anal_files_gz_md5hash'

    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, file_name)
    body = obj.get()['Body'].read()
    md5hash_new = hashlib.md5(body).hexdigest()

    dynamodb = boto3.resource('dynamodb', endpoint_url="https://dynamodb.eu-west-1.amazonaws.com")
    table = dynamodb.Table(table_name)
    # TODO: add check if yesterday's data exist
    response = table.get_item(Key={"date": yesterday})
    md5hash_prior = response["Item"]["md5hash"]
    print(response)
    
    if md5hash_new != md5hash_prior:
        table.put_item(Item= {'date': file_name.split('.')[0],'md5hash':  md5hash_new})
    else:
        s3.Object(bucket, file_name).delete()