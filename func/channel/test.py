import unittest
import json
import os
import sys
sys.path.insert(0, os.environ['PWD'] + '/')

from func.channel.list import handler as channel_list
from func.channel.create import handler as channel_create
from func.channel.get import handler as channel_get

class TestChannelFunc(unittest.TestCase):
    def test_channel_create_success(self):
        ch = {
            'channel_name': 'Chan One',
            'channel_desc': 'Yet Another Channel',
            'sub_fee': 99,
            'owner': 'Visio',
        }
        event = {
            'body': json.dumps(ch)
        }
        resp = channel_create(event, {})
        self.assertEqual(resp['statusCode'], 200)
        self.assertNotEqual(resp['body'], '')

if __name__ == '__main__':
    unittest.main()