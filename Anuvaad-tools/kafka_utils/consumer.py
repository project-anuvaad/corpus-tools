from kafka import KafkaConsumer
from utils.anuvaad_tools_logger import  getLogger
import json
import os

log = getLogger()
kafka_ip_host = 'KAFKA_IP_HOST'
default_value = 'localhost:9092'
local_ip = '172.17.18.200:9092'
bootstrap_server = os.environ.get(kafka_ip_host, default_value)


def get_consumer(topic):
    try:
        consumer = KafkaConsumer(
            topic,
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='anuvaad',
            bootstrap_servers=[bootstrap_server],
            value_deserializer=lambda x: handle_json(x))

        log.info('get_consumer : consumer returned for topic = ' + topic)
        return consumer
    except Exception as e:
        log.error('get_consumer : ERROR OCCURRED for getting consumer with topic = ' + topic)
        log.error('get_consumer : ERROR = ' + str(e))
        print('error')
        return None


def handle_json(x):
    try:
        return json.loads(x.decode('utf-8'))
    except Exception as e:
        log.error('get consumer : ERROR OCCURED for reading message  ' + str(e))
        return {}
