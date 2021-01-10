# from meitouAWS import Channel
import boto3
import logging
import uuid

from data.model.channel import Channel
from botocore.exceptions import ClientError

class DyanmoExecutor:
  def __init__(self, client, table_name):
    self.client = client
    self.table_name = table_name
    logging.basicConfig(format='%(asctime)s - %(levelname)s - %(message)s', level=logging.INFO)
      
  # tester
  def scan_table(self):
    print('scanning table', self.table_name)
    table = self.client.Table(self.table_name)
  
  def delete_channel_by_id(self, channel_id):
    table = self.client.Table(self.table_name)
    try:
      resp = table.delete_item(
        Key={
          'channel_id': channel_id,
          'channel_sk': 'GENERAL_INFO'
        }
      )
      # logging.info(resp)
    except Exception as e:
      logging.error(e)
  
  def get_channel_by_id(self, channel_id):
    table = self.client.Table(self.table_name)
    errMsg = 'default error message, we should never see this'
    statusCode = 400
    try:
      resp = table.get_item(Key={'channel_id': channel_id, 'channel_sk': 'GENERAL_INFO'})
      if resp['ResponseMetadata']['HTTPStatusCode'] == 200 and 'Item' in resp:   
        logging.info(resp['Item'])
        return {
          'statusCode': 200,
          'channel': resp['Item']
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
    table = self.client.Table(self.table_name)
    new_id = str(uuid.uuid4())
    errMsg = 'default error message, we should never see this'
    statusCode = 400
    try:
      resp = table.put_item(
        Item = {
          'channel_id': new_id,
          'channel_sk': 'GENERAL_INFO',
          'channel_name': channel.name,
        }
      )
      if resp['ResponseMetadata']['HTTPStatusCode'] == 200:   
        return {
          'channel_id': new_id,
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
