import utils.anuvaad_constants as Constants
from utils.file_util import read_csv, write_to_csv
from kafka_utils.producer import send_to_kafka
from utils.anuvaad_tools_logger import getLogger

log = getLogger()


def start_composition(processId, selected_file_names, target_language, source_language):
    try:
        log.info('start_composition : started')
        base_path = Constants.BASE_PATH_TOOL_4 + processId + '/'
        source_file_name = processId + '_' + source_language + Constants.SOURCE_TXT
        target_file_name = processId + '_' + target_language + Constants.TARGET_TXT
        merger_file_name = processId + Constants.FINAL_CSV
        source_file_path = base_path + source_file_name
        target_file_path = base_path + target_file_name
        merger_file_path = base_path + merger_file_name
        sentence_count = 0
        for file_name in selected_file_names:
            file_path = base_path + file_name

            sentences = read_csv(file_path, 2)
            sentence_count = sentence_count + write_to_csv(merger_file_path, sentences, Constants.CSV_APPEND)

            with open(source_file_path, Constants.CSV_APPEND, encoding='utf-8', errors="ignore") as source_txt:
                with open(target_file_path, Constants.CSV_APPEND, encoding='utf-8', errors="ignore") as target_txt:
                    for line in sentences:
                        sentence_count = sentence_count + 1
                        source_txt.write(line[Constants.SOURCE] + '\n')
                        target_txt.write(line[Constants.TARGET] + '\n')

        msg = {Constants.PATH: Constants.FILE_MERGER,
               Constants.DATA: {
                   Constants.STATUS: Constants.SUCCESS,
                   Constants.PROCESS_ID: processId,
                   Constants.SESSION_ID: processId,
                   Constants.FILES: merger_file_name,
                   Constants.SOURCE_FILE: source_file_name,
                   Constants.TARGET_FILE: target_file_name,
                   Constants.SENTENCE_COUNT: sentence_count,
               }}

        send_to_kafka(Constants.EXTRACTOR_RESPONSE, msg)
        log.info('start_composition : ended for processId == ' + str(processId))
    except Exception as e:
        log.error('start_composition : ERROR OCCURRED ERROR is == ' + str(e))
        try:
            data = {Constants.PATH: Constants.SEARCH_REPLACE,
                    Constants.DATA: {
                        Constants.STATUS: Constants.FAILED,
                        Constants.PROCESS_ID: processId
                    }}
            send_to_kafka(Constants.ERROR_TOPIC, data)
        except Exception as e:
            log.error('start_composition : Error occurred while getting consumer for topic == ' +
                      str(Constants.ERROR_TOPIC) + ' ERROR is == ' + str(e))
