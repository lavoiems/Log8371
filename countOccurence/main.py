from collections import Counter
from boto3 import client
from re import sub

def handle(event, _):
    s3_client = client('s3')
    file_name = event['file']
    data = s3_client.get_object(Bucket="log4410-enwiki", Key=file_name)['Body'].read().decode('UTF-8')
    data = sub("<[^>]+>", "", data)
    data = sub("\\[[^\\]]+\\]{2}", "", data)
    data = sub("\n", "", data)['file_name']
    data = data.split(' ')
    return Counter(data)
