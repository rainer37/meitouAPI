import boto3
import os

from util.meitoudata.model.message import Message
import util.dynamo as dynamo

channel_table = os.environ['CHANNEL_TABLE']

dynamodb = boto3.resource('dynamodb', os.environ['REGION'])
executor = dynamo.DyanmoExecutor(dynamodb, channel_table)

def handler(event, context):
    conn_id = event['requestContext']['connectionId']
    channel_id = event['headers']['channel_id'];
    print(conn_id, channel_id)
    executor.add_connection_to_channel(conn_id, channel_id)
    return {
        'statusCode': 200
    }