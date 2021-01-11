import boto3
import os

from util.meitoudata.model.channel import Channel
import util.dynamo as dynamo

dynamodb = boto3.resource('dynamodb', os.environ['REGION'])
executor = dynamo.DyanmoExecutor(dynamodb, os.environ['CHANNEL_TABLE'])

def handler(event, context):
    print(event)
    channel_id = event['rawPath'].split('/')[2]
    print(channel_id)
    resp = executor.get_channel_by_id(channel_id)
    print(resp)
    return resp