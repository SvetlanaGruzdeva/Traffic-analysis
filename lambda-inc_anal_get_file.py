import boto3
from urllib3 import PoolManager
from datetime import datetime

def lambda_handler(event, context):

    url='http://opendata.ndw.nu/incidents.xml.gz'
    bucket = 'inc-anal-files-gz'
    key = url.split('/')[-1].replace('incidents', datetime.today().strftime('%d-%m-%Y'))

    s3=boto3.client('s3')

    http = PoolManager()
    try:
        s3.upload_fileobj(http.request('GET', url, preload_content=False), bucket, key)
    except ValueError:
        print('ERROR: Invalid link.')
    except s3.exceptions.NoSuchBucket:
        print('ERROR: No such bucket.')
    print('File saved successfully.')