import boto3
import json
import os

from util.meitoudata.model.channel import Channel
import util.dynamo as dynamo

dynamodb = boto3.resource('dynamodb', os.environ['REGION'])
executor = dynamo.DyanmoExecutor(dynamodb, os.environ['CHANNEL_TABLE'])

required_keys = [
    'channel_name',
    'channel_desc',
    'owner',
    'sub_fee',
]

def missing_required_data(input_data):
    for key in required_keys:
        if key not in input_data:
            return True
    return False

def handler(event, context):
    input_data = json.loads(event['body'])
    if missing_required_data(input_data):
        return {
            "statusCode": 400,
            "body": "could not create channel entry, missing fields"
        }
    
    ch = Channel(input_data['channel_name'], 
                 input_data['channel_desc'], 
                 input_data['owner'], 
                 input_data['sub_fee'])
    resp = executor.add_channel(ch)
    return resp