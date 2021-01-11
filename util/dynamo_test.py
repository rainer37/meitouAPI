import boto3
import unittest
import os
import sys
sys.path.insert(0, os.environ['PWD'] + '/util/meitoudata/model')

import dynamo

# from meitoudata.model.channel import Channel
from channel import Channel

channel_table = os.environ['CHANNEL_TABLE_NAME']
dynamodb = boto3.resource('dynamodb', region_name=os.environ['REGION'])

class TestDynamoExecutor(unittest.TestCase):

    channel_id = ''

    def test_add_channel_success(self):
        ch = Channel('Channel V')
        executor = dynamo.DyanmoExecutor(dynamodb, channel_table)
        resp = executor.add_channel(ch)
        self.assertEqual(resp['statusCode'], 200)
        self.__class__.channel_id = resp['channel_id']

    def test_add_channel_fail_unknown_table(self):
        ch = Channel('Channel V')
        table_name = 'some-random-table'
        executor = dynamo.DyanmoExecutor(dynamodb, table_name)
        resp = executor.add_channel(ch)
        self.assertEqual(resp['statusCode'], 404)
        self.assertEqual(resp['errMsg'], 
            'An error occurred (ResourceNotFoundException) when calling the PutItem operation: Requested resource not found')
        
    def test_get_channel_success(self):
        executor = dynamo.DyanmoExecutor(dynamodb, channel_table)
        resp = executor.get_channel_by_id(self.__class__.channel_id)
        self.assertEqual(resp['statusCode'], 200)

    def test_get_channel_fail_unknown_table(self):
        table_name = 'some-random-table'
        executor = dynamo.DyanmoExecutor(dynamodb, table_name)
        resp = executor.get_channel_by_id('some-id')
        self.assertEqual(resp['statusCode'], 404)
        self.assertEqual(resp['errMsg'], 
            'An error occurred (ResourceNotFoundException) when calling the GetItem operation: Requested resource not found')

    def test_get_channel_failed_no_such_id(self):
        executor = dynamo.DyanmoExecutor(dynamodb, channel_table)
        resp = executor.get_channel_by_id('random-id-0-1-2')
        self.assertEqual(resp['statusCode'], 405)
        self.assertEqual(resp['errMsg'], 'no such channel')
    
    @classmethod
    def tearDownClass(cls):
        print('tear down')
        executor = dynamo.DyanmoExecutor(dynamodb, channel_table)
        resp = executor.delete_channel_by_id(__class__.channel_id)
        print(resp)

if __name__ == '__main__':
    unittest.main()