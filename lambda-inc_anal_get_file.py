import boto3
from urllib3 import PoolManager

def lambda_handler(event, context):

    url='http://opendata.ndw.nu/incidents.xml.gz' # put your url here
    bucket = 'inc-anal-files-gz' #your s3 bucket
    key = 'filename.gz' #your desired s3 path or filename

    s3=boto3.client('s3')

    http = PoolManager()
    s3.upload_fileobj(http.request('GET', url, preload_content=False), bucket, key)