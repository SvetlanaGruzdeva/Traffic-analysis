import json
import hashlib
import boto3
from datetime import datetime, timedelta
from pprint import pprint

def lambda_handler(event, context):
    
    # Define all necessary variables
    bucket = 'events-anal-files-gz'
    file_name = event['Records'][0]['s3']['object']['key']
    prior_date = datetime.strptime(file_name.split('.')[0], '%d-%m-%Y') - timedelta(days=1)
    table_name = 'events_anal_files_gz_md5hash'
    
    # Calculate md5hash for the file which had been just saved in s3 by previous lambda
    s3 = boto3.resource('s3')
    try:
        obj = s3.Object(bucket, file_name)
        body = obj.get()['Body'].read()
        md5hash_new = hashlib.md5(body).hexdigest()
    except boto3.client('s3').exceptions.NoSuchBucket:
        print('ERROR: No such bucket.')
    print(f'md5hash_new is {md5hash_new}.')

    # Get md5hash of the last recording in DynamoDB table
    dynamodb = boto3.resource('dynamodb', endpoint_url="https://dynamodb.eu-west-1.amazonaws.com")
    table = dynamodb.Table(table_name)
    
    md5hash_prior = ''
    while md5hash_prior == '':
        try:
            response = table.get_item(Key={"date": prior_date.strftime('%d-%m-%Y')})
            md5hash_prior = response["Item"]["md5hash"]
        except KeyError:
            print('ERROR: No such date or Item.')
            prior_date = prior_date - timedelta(days=1)
    print(f'The latest available md5hash is {prior_date}: {md5hash_prior}.')
        
    # Compare md5hash for new and the last recorded files
    if md5hash_new != md5hash_prior:
        table.put_item(Item= {'date': file_name.split('.')[0],'md5hash':  md5hash_new})
        record = file_name.split('.')[0]
        print(f'New record has been added:{record}: {md5hash_new}')
    else:
        s3.Object(bucket, file_name).delete()
        print(f'ALERT: File {file_name} is dublicate and has been deleted.')