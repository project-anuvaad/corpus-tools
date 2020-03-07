from mongoengine import *


class ToolProcess(DynamicDocument):
    processId = StringField(required=True)
    status = BooleanField()
    type = StringField()
