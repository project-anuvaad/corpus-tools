from kafka_utils.consumer import get_consumer
from kafka_utils.producer import send_to_kafka
from utils.anuvaad_tools_logger import getLogger
from compositioner.composition import start_composition
from utils import anuvaad_constants as Constants

log = getLogger()


def composition_thread():
    try:
        log.info('composition_thread : started')
        consumer = get_consumer(Constants.COMPOSITION)
        for msg in consumer:
            try:
                log.info('composition_thread : message for queue == ' + str(msg))
                message = msg.value[Constants.DATA]
                path = msg.value[Constants.PATH]
                processId = message[Constants.SESSION_ID]

                if path == Constants.FILE_MERGER:
                    selected_files = message[Constants.SELECTED_FILES]
                    target_language = message[Constants.TARGET_LANGUAGE]
                    source_language = Constants.EN
                    start_composition(processId, selected_files, target_language, source_language)

                log.info('composition_thread : Ended for processId == ' + str(processId))
            except Exception as e:
                log.error('composition_thread : ERROR OCCURRED ERROR is == ' + str(e))
                data = {Constants.PATH: Constants.COMPOSITION,
                        Constants.DATA: {
                            Constants.STATUS: Constants.FAILED,
                            Constants.PROCESS_ID: processId
                        }}
                send_to_kafka(Constants.ERROR_TOPIC, data)

    except Exception as e:
        log.error('composition_thread : Error occurred while getting consumer for topic == ' +
                  str(Constants.ERROR_TOPIC) + ' ERROR is == ' + str(e))
