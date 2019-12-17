import json
from kafka import KafkaProducer
from utils.anuvaad_tools_logger import getLogger
import os

log = getLogger()
kafka_ip_host = 'KAFKA_IP_HOST'
default_value = 'localhost:9092'
local_ip = '172.17.18.200:9092'
bootstrap_server = os.environ.get(kafka_ip_host, default_value)


def get_producer():
    try:
        producer = KafkaProducer(bootstrap_servers=[bootstrap_server],api_version=(1, 0, 0),
                                 value_serializer=lambda x: json.dumps(x).encode('utf-8'))
        log.info('get_producer : producer returned successfully')
        return producer
    except Exception as e:
        log.error('get_producer : ERROR OCCURRED while creating producer, ERROR =  ' + str(e))
        return None
