import json

class Channel:
    def __init__(self, name, desc, owner='system', fee=5):
        self.name = name
        self.desc = desc
        self.fee = fee
        self.owner = owner
        self.id = ''
    
    def set_id(self, id):
        self.id = id
    
    def to_json(self):
        return json.dumps({
            "channel_name": self.name,
            "channel_desc": self.desc,
            "channel_id": self.id,
            "owner": self.owner,
            "sub_fee": self.fee,
        })
