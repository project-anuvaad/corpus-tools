from mongoengine import *


class Corpus(DynamicDocument):
    basename = StringField(required=True)
    created_on = StringField()
    last_modified = StringField()
    comment = StringField()
    author = StringField()
    no_of_sentences = IntField()
    status = StringField()
    domain = StringField()
    name = StringField()
    type = StringField()
