import boto3
import json
import os
import uuid

import util.dynamo as dynamo

MSG_ID = 'msg_id'
chat_table = os.environ['CHAT_TABLE']
channel_table = os.environ['CHANNEL_TABLE']

required_msg_attrs = ['channel_id', 'content', 'sender_id', 'hashtags']
dynamodb = boto3.resource('dynamodb', os.environ['REGION'])
executor_channel = dynamo.DyanmoExecutor(dynamodb, channel_table)
executor_chat = dynamo.DyanmoExecutor(dynamodb, chat_table)

def handler(event, context):
    print(event)
    # parse the message to Message
    # broadcase it to all other conn in the same channel
    try:
        raw_json = json.loads(event['body'])
        if 'message' not in raw_json:
            return resp_bad(400, 'no message here?')
    
        raw_message = json.loads(json.loads(event['body'])['message'])
        # print(raw_message)
    except Exception as e:
        print(e)
        return resp_bad(401, 'invalid json formation')
    
    if is_not_valid_msg(raw_message):
        return resp_bad(402, 'missing attributs in message')
        
    endpoint = get_api_gw_endpoint(event)
    
    # switch on type of message, new message, update old message, etc
    if MSG_ID in raw_message:
        # existing message
        _process_action(raw_message)
    else:
        channel_id = raw_message['channel_id']

        _persist_new_message(raw_message)

        _broadcast_message(json.dumps(raw_message), channel_id, endpoint)
    return {
        'statusCode': 200    
    }

def is_not_valid_msg(msg):
    for attr in required_msg_attrs:
        if attr not in msg or not isinstance(msg[attr], str):
            return True
    return False

# # generate a new id, for now using uuid v4
# def generate_new_msg_id():
#     return str(uuid.uuid4())

def _persist_new_message(raw_message):
    return executor_chat.insert_new_message(raw_message)

# get the endpoint of apigatway to send back the msg
def get_api_gw_endpoint(event):
    domain = event["requestContext"]["domainName"]
    stage = event["requestContext"]["stage"]
    return "https://{0}/{1}".format(domain, stage)

# list all connetion ids from channel table
# connection is prefixed with CONN#
def get_connections_from_channel(channel_id):
    return executor_channel.get_all_connections_in_channel(channel_id)

def _broadcast_message(msg, channel_id, endpoint):

    all_connection_ids = get_connections_from_channel(channel_id)['body']

    apigatewaymanagementapi = boto3.client(
        'apigatewaymanagementapi', 
        endpoint_url = endpoint,
    )
    
    for connection_id in all_connection_ids:
        apigatewaymanagementapi.post_to_connection(
            Data=msg,
            ConnectionId=connection_id.split('#')[1]
        )

def _process_norm(raw_message):
    pass

def _process_action(raw_message):
    msg_id = raw_message['msg_id']

def resp_bad(code, errMsg):
    print(errMsg)
    return {
        'statusCode': code,
    }
