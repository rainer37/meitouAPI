import unittest
import json
import os
import sys
sys.path.insert(0, os.environ['PWD'] + '/')

from util.meitoudata.model.message import Message, InvalidTagException

class TestMessage(unittest.TestCase):

    def test_message_basic(self):
        msg_0 = Message('hello world', 'sender Bob', 'chan-0-1-2', '')
        self.assertEqual(msg_0.content, 'hello world')
        self.assertEqual(msg_0.sender, 'sender Bob')
        self.assertEqual(msg_0.channel_id, 'chan-0-1-2')
        self.assertEqual(msg_0.hashtags, '')
        self.assertFalse(msg_0.is_special_question())
    
    def test_add_tag(self):
        msg_0 = Message('hello world', 'sender Bob', 'chan-0-1-2', '')
        msg_0.add_tag('ATAG')
        self.assertEqual(msg_0.hashtags, 'ATAG')
        msg_0.add_tag('CTAG')
        self.assertEqual(msg_0.hashtags, 'ATAG,CTAG')
        msg_0.add_tag('CTAG')
        self.assertEqual(msg_0.hashtags, 'ATAG,CTAG')
        with self.assertRaises(InvalidTagException): msg_0.add_tag('A B')
        with self.assertRaises(InvalidTagException): msg_0.add_tag('A\nB')
        with self.assertRaises(InvalidTagException): msg_0.add_tag('AB ')
        with self.assertRaises(InvalidTagException): msg_0.add_tag(' AB')
        self.assertFalse(msg_0.is_special_question())


if __name__ == '__main__':
    unittest.main()