from mongoengine import *


class SentencePair(DynamicDocument):
    processId = StringField(required=True)
    source = StringField()
    target = StringField()
    updated = StringField()
    accepted = BooleanField()
    serial_no = IntField()
    changes = ListField()
    in_review = BooleanField()
    review_completed = BooleanField()
    update_corpus = BooleanField()
    hash = StringField()
    is_alone = BooleanField()
    is_written = BooleanField()

