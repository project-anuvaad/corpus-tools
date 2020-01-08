from kafka_utils.consumer import get_consumer
from utils.anuvaad_tools_logger import getLogger
from utils import anuvaad_constants as Constants
from machine_translator.machine_translator import start_machine_translation
from machine_translator.machine_translator import check_and_translate, write_csv_for_translation, merge_files
from kafka_utils.producer import send_to_kafka
from elastic_utils.es_utils import get_all_by_ids, update

log = getLogger()


def machine_translation_thread():
    try:
        log.info('machine_translation_thread : started')
        consumer = get_consumer(Constants.TOPIC_MACHINE_TRANSLATION)
        for msg in consumer:
            try:
                log.info('machine_translation_thread : message for queue == ' + str(msg))
                message = msg.value[Constants.DATA]
                sourceFiles = message[Constants.SELECTED_FILES]
                targetLanguage = message[Constants.TARGET_LANGUAGE]
                processId = message[Constants.SESSION_ID]
                workspace = message[Constants.TITLE]
                created_by = message[Constants.CREATED_BY]
                domain = None
                use_latest = message[Constants.USE_LATEST]
                start_machine_translation(processId, workspace, sourceFiles, targetLanguage, created_by,
                                          None, domain, use_latest)
                log.info('machine_translation_thread : Ended for processId == ' + str(processId))
            except Exception as e:

                log.error('machine_translation_thread : ERROR OCCURRED ERROR is == ' + str(e))
                data = {'path': 'mt',
                 'data': {
                     'status': False,
                     'processId': processId
                 }}
                send_to_kafka(topic=Constants.ERROR_TOPIC, value=data)
    except Exception as e:
        log.error('machine_translation_thread : Error occurred while getting consumer for topic == ' +
                  str(Constants.TOPIC_MACHINE_TRANSLATION) + ' ERROR is == ' + str(e))


def translation_fetcher_and_writer_thread():
    try:
        log.info('translation_fetcher_and_writer_thread : started')
        consumer = get_consumer(Constants.TOPIC_FETCHER)
        for msg in consumer:
            try:
                log.info('translation_fetcher_and_writer_thread : message for queue == ' + str(msg))
                message = msg.value[Constants.DATA]

                type_ = message[Constants.TYPE]
                if type_ == Constants.TRANSLATE:
                    check_and_translate(message)
                elif type_ == Constants.WRITE_CSV:
                    filename_t = write_csv_for_translation(message)
                    process_id = message[Constants.PROCESS_ID]
                    es_response = get_all_by_ids([process_id], Constants.FILE_INDEX)
                    source = es_response[process_id]
                    sent = source[Constants.FILE_COUNT]
                    received = source[Constants.COMPLETE]
                    all_files = source[Constants.FILES]
                    basepath = source[Constants.PATH]
                    if received + 1 == sent:
                        merged_file_name = merge_files(all_files, process_id, basepath)
                        message = {
                            Constants.FILE_NAME: merged_file_name,
                            Constants.STATUS: Constants.SUCCESS,
                            Constants.PROCESS_ID: process_id
                        }
                        data = {Constants.DATA: message}
                        send_to_kafka(topic=Constants.TOPIC_SENTENCES_PROCESSING, value=data)
                    else:
                        body = {Constants.COMPLETE: received + 1}
                        update(process_id, Constants.FILE_INDEX, body)

                    log.info('translation_fetcher_and_writer_thread : process completed for process id == '
                             + str(process_id))

                log.info('translation_fetcher_and_writer_thread : Ended ')
            except Exception as e:
                log.error('translation_fetcher_and_writer_thread : ERROR OCCURRED ERROR is == ' + str(e))
                message = {
                    Constants.STATUS: Constants.FAILED,
                    Constants.PROCESS_ID: process_id
                }
                data = {Constants.DATA: message}
                send_to_kafka(topic=Constants.ERROR_TOPIC, value=data)

    except Exception as e:
        log.error('translation_fetcher_and_writer_thread : Error occurred while getting consumer for topic == ' +
                  str(Constants.TOPIC_FETCHER) + ' ERROR is == ' + str(e))


