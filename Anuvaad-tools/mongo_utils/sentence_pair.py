from mongoengine import *


class SentencePair(DynamicDocument):
    processId = StringField(required=True)
    source = StringField()
    target = StringField()
    source_search = StringField()
    target_search = StringField()
    replace = StringField()
    updated = StringField()
    accepted = BooleanField()
    serial_no = IntField()
