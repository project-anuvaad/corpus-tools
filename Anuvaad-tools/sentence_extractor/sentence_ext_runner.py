from kafka_utils.consumer import get_consumer
from utils.anuvaad_tools_logger import getLogger
from sentence_extractor.sentence_extractor import start_sentence_extraction
from sentence_extractor import extractor_constants as Constants

TOPIC_TOKEN_EXTRACTOR = 'sentencesext'
log = getLogger()


def extract_sentences_thread():
    try:
        log.info('extract_sentences_thread : started')
        consumer = get_consumer(TOPIC_TOKEN_EXTRACTOR)
        for msg in consumer:
            try:
                log.info('extract_sentences_thread : message for queue == ' + str(msg))
                message = msg.value[Constants.DATA]
                configFilePath = message[Constants.CONFIG_FILE_LOCATION]
                posTokenFilePath = message[Constants.TOKEN_FILE]
                negTokenFilePath = message[Constants.NEGATIVE_TOKEN_FILE]
                paragraphFilePath = message[Constants.PARAGRAPH_FILE_LOCATION]
                processId = message[Constants.SESSION_ID]
                workspace = message[Constants.TITLE]

                start_sentence_extraction(configFilePath, posTokenFilePath, negTokenFilePath, paragraphFilePath, processId, workspace, message)
                log.info('extract_sentences_thread : Ended for processId == '+str(processId))
            except Exception as e:
                log.error('extract_sentences_thread : ERROR OCCURRED for processId == ' +
                          str(processId) + 'ERROR is == ' + str(e))

    except Exception as e:
        log.error('extract_sentences_thread : Error occurred while getting consumer for topic == '
                  + str(TOPIC_TOKEN_EXTRACTOR) + ' ERROR is == ' + str(e))
