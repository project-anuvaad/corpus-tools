from kafka_utils.consumer import get_consumer
from kafka_utils.producer import send_to_kafka
from utils.anuvaad_tools_logger import getLogger
from search_replace.search_and_replace import start_search_replace, write_to_file
from utils import anuvaad_constants as Constants

log = getLogger()


def search_replace_thread():
    try:
        log.info('search_replace_thread : started')
        consumer = get_consumer(Constants.TOPIC_SEARCH_REPLACE)
        for msg in consumer:
            try:
                log.info('search_replace_thread : message for queue == ' + str(msg))
                message = msg.value[Constants.DATA]
                path = msg.value[Constants.PATH]
                processId = message[Constants.SESSION_ID]
                if path == Constants.SEARCH_REPLACE:
                    username = message[Constants.USERNAME]
                    workspace = message[Constants.TITLE]
                    configFilePath = message[Constants.CONFIG_FILE_LOCATION]
                    selected_files = message[Constants.SELECTED_FILES]
                    target_language = message[Constants.TARGET_LANGUAGE]
                    source_language = message[Constants.SOURCE_LANGUAGE]
                    start_search_replace(processId, workspace, configFilePath, selected_files, username,
                                         source_language, target_language)
                elif path == Constants.WRITE_TO_FILE:
                    username = message[Constants.USERNAME]
                    workspace = message[Constants.TITLE]
                    target_language = message[Constants.TARGET_LANGUAGE]
                    source_language = message[Constants.SOURCE_LANGUAGE]

                    write_to_file(processId, username, workspace, target_language, source_language)

                log.info('search_replace_thread : Ended for processId == '+str(processId))
            except Exception as e:
                log.error('search_replace_thread : ERROR OCCURRED ERROR is == ' + str(e))
                data = {Constants.PATH: Constants.SEARCH_REPLACE,
                        Constants.DATA: {
                            Constants.STATUS: Constants.FAILED,
                            Constants.PROCESS_ID: processId
                        }}
                send_to_kafka(Constants.ERROR_TOPIC, data)

    except Exception as e:
        log.error('search_replace_thread : Error occurred while getting consumer for topic == ' +
                  str(Constants.ERROR_TOPIC) + ' ERROR is == ' + str(e))
