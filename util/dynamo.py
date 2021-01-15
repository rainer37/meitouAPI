import boto3
import json
import logging
import time
import uuid

from botocore.exceptions import ClientError
from util.meitoudata.model.channel import Channel

class DyanmoExecutor:
  def __init__(self, client, table_name):
    self.client = client
    self.table = self.client.Table(table_name)
    logging.basicConfig(format='%(asctime)s - %(levelname)s %(message)s', level=logging.INFO)
      
  # tester
  def scan_table(self):
    print('scanning table', self.table_name)
  
  def delete_channel_by_id(self, channel_id):
    try:
      resp = self.table.delete_item(
        Key={
          'channel_id': channel_id,
          'channel_sk': 'GENERAL_INFO',
        }
      )
      # logging.info(resp)
    except Exception as e:
      logging.error(e)
  
  def get_channel_by_id(self, channel_id):
    errMsg = 'default error message, we should never see this'
    statusCode = 400
    try:
      resp = self.table.get_item(Key={'channel_id': channel_id, 'channel_sk': 'GENERAL_INFO'})
      if resp['ResponseMetadata']['HTTPStatusCode'] == 200 and 'Item' in resp:   
        # logging.info(resp['Item'])
        item = resp['Item']
        ch = Channel(item['channel_name'], item['channel_desc'], item['owner'], int(item['sub_fee']))
        ch.set_id(item['channel_id'])

        return {
          'statusCode': 200,
          'body': ch.to_json()
        }
      else:
        statusCode = 405
        errMsg = 'no such channel'
    except Exception as e:
      logging.error(e)
      errMsg = str(e)
      statusCode = 404

    return {
      'statusCode': statusCode,
      'errMsg': errMsg
    }
  
  def add_channel(self, channel):
    logging.info("adding channel {0}".format(channel.name))
    new_id = str(uuid.uuid4())
    errMsg = 'default error message, we should never see this'
    statusCode = 400
    try:
      resp = self.table.put_item(
        Item = {
          'channel_id': new_id,
          'channel_sk': 'GENERAL_INFO',
          'channel_name': channel.name,
          'channel_desc': channel.desc,
          'owner': channel.owner,
          'sub_fee': channel.fee
        }
      )
      if resp['ResponseMetadata']['HTTPStatusCode'] == 200:   
        return {
          'body': new_id,
          'statusCode': 200
        }
      else:
        errMsg = 'unknown http status code ' +  resp['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
      logging.error(e)
      errMsg = str(e)
      statusCode = 404

    return {
      'statusCode': statusCode,
      'errMsg': errMsg
    }

  def delete_message_by_id(self, channel_id, msg_sk):
    try:
      resp = self.table.delete_item(
        Key={
          'channel_id': channel_id,
          'msg_sk': msg_sk,
        }
      )
      # logging.info(resp)
    except Exception as e:
      logging.error(e)

  def insert_new_message(self, raw_message):
    # new_msg_id = str(uuid.uuid4())
    time_now = str(time.time())
    channel_id = raw_message['channel_id']
    sender_id = raw_message['sender_id']
    msg_sk = time_now + '#' + sender_id
    content = raw_message['content']
    hashtags = raw_message['hashtags']
    
    errMsg = 'default error message, we should never see this'
    statusCode = 400
    
    try:
      resp = self.table.put_item(
        Item = {
          'channel_id':  channel_id,
          'msg_sk': msg_sk,
          'content': content,
          'sender_id': sender_id,
          'hashtags': hashtags,
          'last_updated_at': time_now,
        }
      )
      if resp['ResponseMetadata']['HTTPStatusCode'] == 200:   
        return {
          'body': msg_sk,
          'statusCode': 200
        }
      else:
        errMsg = 'unknown http status code ' +  resp['ResponseMetadata']['HTTPStatusCode']
    except Exception as e:
      logging.error('error while inserting message')
      logging.error(e)
      errMsg = str(e)
      statusCode = 404
      
    return {
      'statusCode': statusCode,
      'errMsg': errMsg
    }
  
  def get_message(self, channel_id, msg_sk):
    errMsg = 'default error message, we should never see this'
    statusCode = 400
    try:
      resp = self.table.get_item(Key={'channel_id': channel_id, 'msg_sk': msg_sk})
      if resp['ResponseMetadata']['HTTPStatusCode'] == 200 and 'Item' in resp:   
        # logging.info(resp['Item'])
        item = resp['Item']
        msg = {
          'channel_id': item['channel_id'],
          'msg_sk': item['msg_sk'],
          'content': item['content'],
          'sender_id': item['sender_id'],
          'last_updated_at': item['last_updated_at'],
          'hashtags': item['hashtags'],
        }

        return {
          'statusCode': 200,
          'body': json.dumps(msg)
        }
      else:
        statusCode = 405
        errMsg = 'no such message'
    except Exception as e:
      logging.error(e)
      errMsg = str(e)
      statusCode = 404

    return {
      'statusCode': statusCode,
      'errMsg': errMsg
    }     
  
  def get_all_message_in_channel(self, channel_id):
    pass