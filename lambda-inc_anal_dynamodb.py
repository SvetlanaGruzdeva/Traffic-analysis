import json
import hashlib
import boto3
from datetime import date, timedelta

def lambda_handler(event, context):

    today = date.today().strftime('%d-%m-%Y')
    yesterday = (date.today() - timedelta(days=1)).strftime('%d-%m-%Y')
    bucket = 'inc-anal-files-gz'
    file_name = today+'.xml.gz'   # To get this passed from another lambda
    table_name = 'inc_anal_files_gz_md5hash'

    # Calculate md5hash for new file
    s3 = boto3.resource('s3')
    obj = s3.Object(bucket, file_name)
    body = obj.get()['Body'].read()
    md5hash_new = hashlib.md5(body).hexdigest()

    # Extract md5hash recort for the latest file
    dynamodb = boto3.resource('dynamodb', endpoint_url="https://dynamodb.eu-west-1.amazonaws.com")
    table = dynamodb.Table(table_name)
    response = table.get_item(Key={"date": yesterday})
    md5hash_prior = response["Item"]["md5hash"]
    
    # Compare md5hash for two files
    if md5hash_new != md5hash_prior:
        table.put_item(Item= {'date': today,'md5hash':  md5hash_new})
    else:
        s3.Object(bucket, file_name).delete()