from kafka_utils.consumer import get_consumer
from utils.anuvaad_tools_logger import getLogger
from sentence_extractor.token_extractor import start_token_extraction

TOPIC_TOKEN_EXTRACTOR = 'tokenext'
log = getLogger()


def extract_tokens_thread():
    try:
        log.info('extract_tokens_thread : started')
        consumer = get_consumer(TOPIC_TOKEN_EXTRACTOR)
        for msg in consumer:
            try:
                log.info('extract_tokens_thread : message for queue == ' + str(msg))
                message = msg.value['data']
                configFilePath = message['config_file_location']
                paragraphFilePath = message['csv_file_location']
                processId = message['session_id']
                # configFilePath = '/home/mayank/PycharmProjects/Anuvaad-tools/resources/tool_1_config.yaml'
                # paragraphFilePath = '/home/mayank/PycharmProjects/Anuvaad-tools/resources/raw_para.csv'
                start_token_extraction(configFilePath, paragraphFilePath, processId)
                log.info('extract_tokens_thread : Ended for processId == '+str(processId))
            except Exception as e:
                log.error('extract_tokens_thread : ERROR OCCURRED ERROR is == ' + str(e))

    except Exception as e:
        log.error('extract_tokens_thread : Error occurred while getting consumer for topic == ' +
                  str(TOPIC_TOKEN_EXTRACTOR) + ' ERROR is == ' + str(e))
