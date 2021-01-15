import boto3
import json
import os
import uuid

from util.meitoudata.model.Message import Message
import util.dynamo as dynamo


MSG_ID = 'msg_id'
chat_table = os.environ['ChatHistoryTable']
channel_table = os.environ['CHANNEL_TABLE_NAME']

required_msg_attrs = ['channel_id', 'content', 'sender_id', 'hashtags']
dynamodb = boto3.client('dynamodb')
executor = dynamo.DyanmoExecutor(dynamodb, os.environ['CHANNEL_TABLE'])

def handler(event, context):
    print(event)
    # parse the message to Message
    # broadcase it to all other conn in the same channel
    try:
        raw_json = json.loads(event['body'])
        if 'message' not in raw_json:
            return resp_bad(400, 'no message here?')
    
        raw_message = json.loads(json.loads(event['body'])['message'])
        print(raw_message)
    except Exception as e:
        print(e)
        return resp_bad(401, 'invalid json formation')
    
    if is_not_valid_msg(raw_message):
        return resp_bad(402, 'missing attributs in message')
        
    # switch on type of message, new message, update old message, etc
    if MSG_ID in raw_message:
        # existing message
        _process_action(raw_message)
    else:
        new_msg_id = str(uuid.uuid4())
        channel_id = raw_message['channel_id']
        sender_id = raw_message['sender_id']
        content = raw_message['content']
        hashtags = raw_message['hashtags']
        
        endpoint = get_api_gw_endpoint(event)
        _persist_new_message(raw_message)
        
        # inject new message id into message to send back
        raw_message['msg_id'] = new_msg_id
        _broadcast_message(raw_message, channel_id, endpoint)

def is_not_valid_msg(msg):
    for attr in required_msg_attrs:
        if attr not in msg or not isinstance(msg[attr], str):
            return False
    return True

# generate a new id, for now using uuid v4
def generate_new_msg_id():
    return str(uuid.uuid4())

def _persist_new_message(raw_message):
    return executor.insert_new_message(raw_message)

# get the endpoint of apigatway to send back the msg
def get_api_gw_endpoint(event):
    domain = event["requestContext"]["domainName"]
    stage = event["requestContext"]["stage"]
    return "https://{0}/{1}".format(domain, stage)

# list all connetion ids from channel table
# connection is prefixed with CONN#
def get_connections_from_channel(channel_id):
    resp = dynamo.query(
        TableName=channel_table,
        KeyConditionExpression='channel_id = :cid AND begins_with(channel_sk, :conhash)',
        ExpressionAttributeValues={
            ':cid': {'S': channel_id},
            ':conhash': {'S': 'CONN#'},
        }
    )
    print(resp['Items'])
    return resp['Items']

def _broadcast_message(msg, channel_id, endpoint):

    all_connection_id = get_connections_from_channel(channel_id)

    apigatewaymanagementapi = boto3.client(
        'apigatewaymanagementapi', 
        endpoint_url = endpoint,
    )
    
    for connectionId in all_connection_id:
        apigatewaymanagementapi.post_to_connection(
            Data=msg,
            ConnectionId=connectionId['channel_sk']['S'].split('#')[1]
        )

def _process_norm(raw_message):
    pass

def _process_action(raw_message):
    msg_id = raw_message['msg_id']

def resp_bad(code, errMsg):
    return {
        'statusCode': code,
        'body': errMsg,
    }
