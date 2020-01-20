from mongoengine import *


class SentencePairUnchecked(Document):
    processId = StringField(required=True)
    source = StringField()
    target = StringField()
    serial_no = IntField()
