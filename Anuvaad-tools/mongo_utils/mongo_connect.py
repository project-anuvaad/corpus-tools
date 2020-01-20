from mongoengine import *
import os
import utils.anuvaad_constants as Constants

mongo_ip = 'MONGO_IP'
default_value = 'localhost'
mongo_server = os.environ.get(mongo_ip, default_value)
PORT = 27017


def connect_mongo():
    connect(Constants.SEARCH_REPLACE_DB, host=mongo_server, port=PORT)
