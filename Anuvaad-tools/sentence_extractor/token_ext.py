from kafka_utils.consumer import get_consumer
from utils.anuvaad_tools_logger import getLogger
from sentence_extractor.token_extractor import start_token_extraction
from sentence_extractor import extractor_constants as Constants

TOPIC_TOKEN_EXTRACTOR = 'tokenext'
log = getLogger()


def extract_tokens():
    try:
        log.info('extract_tokens_thread : started')
        processId = '1234'
        workspace = 'test'
        configFilePath = 'tool_1_config.yaml'
        paragraphFilePath = 'raw_para.csv'
        start_token_extraction(configFilePath, paragraphFilePath, processId, workspace, None)
        log.info('extract_tokens_thread : Ended for processId == '+str(processId))

    except Exception as e:
        log.error('extract_tokens_thread : Error occurred while getting consumer for topic == ' +
                  str(TOPIC_TOKEN_EXTRACTOR) + ' ERROR is == ' + str(e))
