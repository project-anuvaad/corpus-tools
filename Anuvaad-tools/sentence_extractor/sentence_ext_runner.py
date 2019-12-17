from kafka_utils.consumer import get_consumer
from utils.anuvaad_tools_logger import getLogger
from sentence_extractor.sentence_extractor import start_sentence_extraction

TOPIC_TOKEN_EXTRACTOR = 'sentencesext'
log = getLogger()


def extract_sentences_thread():
    try:
        log.info('extract_sentences_thread : started')
        consumer = get_consumer(TOPIC_TOKEN_EXTRACTOR)
        for msg in consumer:
            try:
                log.info('extract_sentences_thread : message for queue == ' + str(msg))
                message = msg.value['data']
                configFilePath = message['config_file_location']
                posTokenFilePath = message['positive_token_file_location']
                negTokenFilePath = message['negative_token_file_location']
                processId = message['session_id']

                start_sentence_extraction(configFilePath, posTokenFilePath, negTokenFilePath, processId)
                log.info('extract_sentences_thread : Ended for processId == '+str(processId))
            except Exception as e:
                log.error('extract_sentences_thread : ERROR OCCURRED for processId == ' +
                          str(processId) + 'ERROR is == ' + str(e))

    except Exception as e:
        log.error('extract_sentences_thread : Error occurred while getting consumer for topic == '
                  + str(TOPIC_TOKEN_EXTRACTOR) + ' ERROR is == ' + str(e))
