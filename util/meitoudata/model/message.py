SPECIAL_QUESTION_TAG = 'SQ'
IMAGE_TAG = 'IMG'
PRIVACY_TAG = 'PRIV'

class InvalidTagException(Exception):
    pass

class Message:
    
    reversed_tags = [SPECIAL_QUESTION_TAG, IMAGE_TAG, PRIVACY_TAG]
    
    def __init__(self, content, sender, channel_id, hashtags=""):
        self.content = content
        self.sender = sender
        self.channel_id = channel_id
        self.hashtags = hashtags
        
    def add_tag(self, tag):
        '''Add a new tag to hashtags string
        
        Add the new tag to hashtag string if not already there, comma separated.
        Raise Exception if the tag contains space or newline
        
        '''
        if " " in tag or "\n" in tag:
            raise InvalidTagException('Tag should not contain space or newline')
        
        if self.hashtags == "":
            self.hashtags = tag
            return
        
        if tag not in self.hashtags.split(','):
            self.hashtags += ",{0}".format(tag)

    def is_special_question(self):
        '''Return True if the message is a special question
        
        Special question is identified by having a SQ tag in hashtag string
        '''
        return SPECIAL_QUESTION_TAG in self.hashtags.split(',')