import unittest
import os

from list import handler as channel_list
from get import handler as channel_get

class TestChannelFunc(unittest.TestCase):
    def test_channel_list(self):
        os.environ['CHANNEL_TABLE'] = 'channels-134507892782-dev'
        os.environ['REGION'] = 'us-west-2'
        event = {}
        context = {}
        resp = channel_list(event, context)
        self.assertEqual(resp['statusCode'], 200)
        self.assertEqual(resp['body'], 'channels listed')

if __name__ == '__main__':
    unittest.main()