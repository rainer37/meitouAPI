
class Channel:
    def __init__(self, name, desc, owner='system', fee=5):
        self.name = name
        self.desc = desc
        self.fee = fee
        self.owner = owner
