import base64
import boto3
import datetime
import os

def lambda_handler(event, context):
  for record in event['Records']:
    #Kinesis data is base64 encoded so decode here
    payload=base64.b64decode(record["kinesis"]["data"])
  
    write_to_s3(payload)
    print("Object successfully stored in S3.")
       
def write_to_s3(data):
    # Upload JSON String to an S3 Object
    client = boto3.client('s3')
    
    timestamp = datetime.datetime.now()
    year = timestamp.strftime("%Y")
    month = timestamp.strftime("%m")
    day = timestamp.strftime("%d")
    
    client.put_object(
        Bucket=os.environ['BUCKET_NAME'], 
        Key=f'source_data/current/{year}/{month}/{day}/{str(datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S"))}.json',
        Body=data
    )