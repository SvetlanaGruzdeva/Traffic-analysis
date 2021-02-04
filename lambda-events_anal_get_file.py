import boto3
from urllib3 import PoolManager
from datetime import datetime

def lambda_handler(event, context):

    url='http://opendata.ndw.nu/gebeurtenisinfo.xml.gz'
    bucket = 'events-anal-files-gz'
    key = url.split('/')[-1].replace('gebeurtenisinfo', datetime.today().strftime('%d-%m-%Y'))

    s3=boto3.client('s3')

    http = PoolManager()
    s3.upload_fileobj(http.request('GET', url, preload_content=False), bucket, key)