from mongoengine import *


class SentencePairUnchecked(DynamicDocument):
    processId = StringField(required=True)
    source = StringField()
    target = StringField()
    serial_no = IntField()
