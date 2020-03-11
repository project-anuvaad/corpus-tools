from kafka_utils.consumer import get_consumer
from kafka_utils.producer import send_to_kafka
from utils.anuvaad_tools_logger import getLogger
from sentence_extractor.token_extractor import start_token_extraction
from utils import anuvaad_constants as Constants
from sentence_extractor.sentence_extractor import start_sentence_extraction

log = getLogger()


# this thread listen to topics for tool 1
def tokens_and_sentences_ext_thread():
    try:
        log.info('tokens_and_sentences_ext_thread : started')
        consumer = get_consumer([Constants.TOPIC_SENTENCE_EXTRACTOR, Constants.TOPIC_TOKEN_EXTRACTOR])
        for msg in consumer:
            if msg.topic == Constants.TOPIC_TOKEN_EXTRACTOR:
                tokens_extraction(msg)
            elif msg.topic == Constants.TOPIC_SENTENCE_EXTRACTOR:
                sentences_extractor(msg)

    except Exception as e:
        log.error('tokens_and_sentences_ext_thread : Error occurred while getting consumer for topic == ' +
                  str(Constants.TOPIC_SENTENCE_EXTRACTOR) + ' ERROR is == ' + str(e))


def tokens_extraction(msg):
    log.info('tokens_extraction : message for queue == ' + str(msg))
    message = msg.value[Constants.DATA]
    processId = message[Constants.SESSION_ID]
    try:
        configFilePath = message[Constants.CONFIG_FILE_LOCATION]
        paragraphFilePath = message[Constants.PARAGRAPH_FILE_LOCATION]
        processId = message[Constants.SESSION_ID]
        workspace = message[Constants.TITLE]
        start_token_extraction(configFilePath, paragraphFilePath, processId, workspace, message)
        log.info('tokens_extraction : Ended for processId == ' + str(processId))
    except Exception as e:
        log.error('tokens_extraction : ERROR OCCURRED ERROR is == ' + str(e))
        data = {Constants.PATH: Constants.TOKEN_EXTRACTOR,
                Constants.DATA: {
                    Constants.STATUS: Constants.FAILED,
                    Constants.PROCESS_ID: processId
                }}
        send_to_kafka(Constants.ERROR_TOPIC, data)


def sentences_extractor(msg):
    log.info('sentences_extractor : message for queue == ' + str(msg))
    message = msg.value[Constants.DATA]
    processId = message[Constants.SESSION_ID]
    try:
        configFilePath = message[Constants.CONFIG_FILE_LOCATION]
        posTokenFilePath = message[Constants.TOKEN_FILE]
        negTokenFilePath = message[Constants.NEGATIVE_TOKEN_FILE]
        paragraphFilePath = message[Constants.PARAGRAPH_FILE_LOCATION]
        workspace = message[Constants.TITLE]

        start_sentence_extraction(configFilePath, posTokenFilePath, negTokenFilePath, paragraphFilePath, processId,
                                  workspace, message)
        log.info('sentences_extractor : Ended for processId == ' + str(processId))
    except Exception as e:
        log.error('sentences_extractor : ERROR OCCURRED for processId == ' +
                  'ERROR is == ' + str(e))
        data = {Constants.PATH: Constants.TOPIC_SENTENCE_EXTRACTOR,
                Constants.DATA: {
                    Constants.STATUS: Constants.FAILED,
                    Constants.PROCESS_ID: processId
                }}
        send_to_kafka(Constants.ERROR_TOPIC, data)
