from kafka_utils.consumer import get_consumer
from kafka_utils.producer import send_to_kafka
from utils.anuvaad_tools_logger import getLogger
from search_replace_and_composition.search_replace.search_and_replace import start_search_replace, write_to_file, \
    write_human_processed_corpus
from utils import anuvaad_constants as Constants
from search_replace_and_composition.compositioner.composition import start_composition
from mongo_utils.tool_process import ToolProcess

log = getLogger()


def search_replace_and_composition_thread():
    try:
        log.info('search_replace_and_composition_thread : started')
        consumer = get_consumer([Constants.TOPIC_SEARCH_REPLACE, Constants.COMPOSITION])

        for msg in consumer:
            log.info('search_replace_and_composition_thread : message is == ' + str(msg))

            if msg.topic == Constants.TOPIC_SEARCH_REPLACE:
                search_replace(msg)
            elif msg.topic == Constants.COMPOSITION:
                composition(msg)

    except Exception as e:
        log.error('search_replace_and_composition_thread : Error occurred while getting consumer for topic == ' +
                  str(Constants.ERROR_TOPIC) + ' ERROR is == ' + str(e))


def composition(msg):
    log.info('composition : started')
    message = msg.value[Constants.DATA]
    path = msg.value[Constants.PATH]
    processId = message[Constants.SESSION_ID]
    if path == Constants.FILE_MERGER:
        try:
            processes = ToolProcess.objects(processId=processId, type=Constants.FILE_MERGER)
            if len(processes) == 0:
                process = ToolProcess(processId=processId, status=False, type=Constants.SEARCH_REPLACE)
                process.save()
                selected_files = message[Constants.SELECTED_FILES]
                target_language = message[Constants.TARGET_LANGUAGE]
                source_language = Constants.EN
                start_composition(processId, selected_files, target_language, source_language)

            else:
                log.info('composition : process with id '
                         + processId + ' is already completed for ' + Constants.SEARCH_REPLACE)

            log.info('composition : ended')
        except Exception as e:
            log.error('composition : for composition :' +
                      ' ERROR OCCURRED ERROR is == ' + str(e))
            data = {Constants.PATH: Constants.COMPOSITION,
                    Constants.DATA: {
                        Constants.STATUS: Constants.FAILED,
                        Constants.PROCESS_ID: processId
                    }}
            send_to_kafka(Constants.ERROR_TOPIC, data)


def search_replace(msg):
    try:
        log.info('search_replace : started ')
        message = msg.value[Constants.DATA]
        path = msg.value[Constants.PATH]
        processId = message[Constants.SESSION_ID]
        if path == Constants.SEARCH_REPLACE:
            username = message[Constants.USERNAME]
            workspace = message[Constants.TITLE]
            configFilePath = message[Constants.CONFIG_FILE_LOCATION]
            selected_files = message[Constants.SELECTED_FILES]
            target_language = message[Constants.TARGET_LANGUAGE]
            source_language = Constants.EN
            processes = ToolProcess.objects(processId=processId, type=Constants.SEARCH_REPLACE)
            if len(processes) == 0:
                process = ToolProcess(processId=processId, status=False, type=Constants.SEARCH_REPLACE)
                process.save()
                start_search_replace(processId, workspace, configFilePath, selected_files, username,
                                     source_language, target_language)
                ToolProcess.objects(processId=processId, type=Constants.SEARCH_REPLACE).update(status=True)
            else:
                log.info('search_replace : process with id '
                         + processId + ' is already completed for ' + Constants.SEARCH_REPLACE)
        elif path == Constants.WRITE_TO_FILE:
            username = message[Constants.USERNAME]
            workspace = message[Constants.TITLE]
            target_language = message[Constants.TARGET_LANGUAGE]
            source_language = Constants.EN
            processes = ToolProcess.objects(processId=processId, type=Constants.WRITE_TO_FILE)
            if len(processes) == 0:
                process = ToolProcess(processId=processId, status=False, type=Constants.WRITE_TO_FILE)
                process.save()
                write_to_file(processId, username, workspace, target_language, source_language)
                ToolProcess.objects(processId=processId, type=Constants.WRITE_TO_FILE).update(status=True)
            else:
                log.info('search_replace : process with id '
                         + processId + ' is already completed for ' + Constants.WRITE_TO_FILE)

        elif path == Constants.HUMAN_CORRECTION:
            write_human_processed_corpus(processId)

        log.info('search_replace : Ended for processId == ' + str(processId))
    except Exception as e:
        log.error('search_replace : ERROR OCCURRED ERROR is == ' + str(e))
        data = {Constants.PATH: Constants.SEARCH_REPLACE,
                Constants.DATA: {
                    Constants.STATUS: Constants.FAILED,
                    Constants.PROCESS_ID: processId
                }}
        send_to_kafka(Constants.ERROR_TOPIC, data)
