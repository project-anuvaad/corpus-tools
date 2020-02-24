from mongoengine import *


class Sentence(DynamicDocument):
    basename = StringField(required=True)
    source = StringField()
    target = StringField()
    status = StringField()
    corpusid = StringField()
    hash = StringField()
    completed = BooleanField()
