import boto3
import os

from util.meitoudata.model.message import Message
import util.dynamo as dynamo

channel_table = os.environ['CHANNEL_TABLE']

dynamodb = boto3.resource('dynamodb', os.environ['REGION'])
executor = dynamo.DyanmoExecutor(dynamodb, channel_table)

def handler(event, context):
    print(event)
    conn_id = event['requestContext']['connectionId']
    return {}
