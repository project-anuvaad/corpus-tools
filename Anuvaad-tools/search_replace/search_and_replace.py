from utils.anuvaad_tools_logger import getLogger
from utils.timeutils import get_current_time
from utils.config_reader import read_config_file
import utils.anuvaad_constants as Constants
from kafka_utils.producer import send_to_kafka
import csv
from mongo_utils.sentence_pair import SentencePair
from mongo_utils.sentence_pair_unchecked import SentencePairUnchecked

log = getLogger()


def start_search_replace(processId, workspace, configFilePath, selected_files):
    start_time = get_current_time()
    log.info('start_search_replace : started at ' + str(start_time))
    configFilePath = Constants.BASE_PATH_TOOL_3 + processId + '/' + configFilePath
    config = read_config_file(configFilePath)
    search_replaces = config[Constants.SEARCH_REPLACE]
    file_count = 1
    try:
        for file in selected_files:
            process(search_replaces, processId, workspace, config, file, file_count)
            file_count = file_count + 1
        data = {Constants.PATH: Constants.SEARCH_REPLACE,
                Constants.DATA: {
                    Constants.STATUS: Constants.SUCCESS,
                    Constants.PROCESS_ID: processId,
                }}
        send_to_kafka(Constants.EXTRACTOR_RESPONSE, data)
    except Exception as e:
        log.error('start_search_replace : Error occurred while processing files, Error is ==  ' + str(e))
        data = {Constants.PATH: Constants.SEARCH_REPLACE,
                Constants.DATA: {
                    Constants.STATUS: Constants.FAILED,
                    Constants.PROCESS_ID: processId,
                }}
        send_to_kafka(Constants.ERROR_TOPIC, data)
    end_time = get_current_time()
    total_time = end_time - start_time
    log.info('start_search_replace : ended at == ' + str(end_time) + ', Total time elapsed == ' + str(total_time))


def process(search_replaces, processId, workspace, config, file, file_count):
    start_time = get_current_time()
    log.info('process : started at ' + str(start_time))
    for search_replace in search_replaces:
        eng_text = search_replace[Constants.ENGLISH]
        translated_text = search_replace[Constants.TRANSLATED]
        replace_text = search_replace[Constants.REPLACE]
        search(file, eng_text, translated_text, replace_text, processId, file_count)

    end_time = get_current_time()
    total_time = end_time - start_time
    log.info('process : ended at == ' + str(end_time) + ', Total time elapsed == ' + str(total_time))


def search(file, eng_text, translated_text, replace_text, processId, file_count):
    start_time = get_current_time()
    log.info('search : started at ' + str(start_time))
    lines = readfile(processId, file)
    line_count = 1
    for line in lines:
        source = line['source']
        target = line['target']
        if source.find(eng_text) > -1 and target.find(translated_text) > -1:
            new_target = target.replace(translated_text, replace_text)
            serial_no = file_count * 1000 + line_count
            create_entry(processId, eng_text, translated_text, new_target, replace_text, source, target, serial_no)
            line_count = line_count + 1
        else:
            serial_no = file_count * 1000 + line_count
            line_count = line_count + 1
            sen = SentencePairUnchecked(processId=processId, source=source, target=target, serial_no=serial_no)
            sen.save()

    end_time = get_current_time()
    total_time = end_time - start_time
    log.info('search : ended at == ' + str(end_time) + ', Total time elapsed == ' + str(total_time))


def create_entry(processId, eng_text, translated_text, target_update, replace_text, source, target, serial_no):
    sen = SentencePair(processId=processId, source_search=eng_text, target_search=translated_text, replace=replace_text,
                       updated=target_update, accepted=False, source=source, target=target, serial_no=serial_no)
    sen.save()


def readfile(processId, file):
    filepath = Constants.BASE_PATH_TOOL_3 + processId + '/' + file
    log.info('readfile  ==  filename  == ' + str(filepath))
    data = list()
    try:
        with open(filepath, Constants.CSV_RT) as file:
            lines = csv.reader(file)
            for line in lines:
                data.append({'source': line[0], 'target': line[1]})
            return data
    except Exception as e:
        log.info('readfile : error occurred while reading file, Error is == ' + str(e))


def write_to_file(processId):
    start_time = get_current_time()
    log.info('write_to_file : started at ' + str(start_time))
    try:
        sentences = SentencePairUnchecked.objects(processId=processId)
        data = []
        for sentence in sentences:
            res = {Constants.SOURCE: sentence[Constants.SOURCE], Constants.TARGET: sentence[Constants.TARGET]}
            data.append(res)
        sentences = SentencePair.objects(processId=processId, accepted=True)
        for sentence in sentences:
            res = {Constants.SOURCE: sentence[Constants.SOURCE], Constants.TARGET: sentence[Constants.UPDATED]}
            data.append(res)
        filepath = Constants.BASE_PATH_TOOL_3 + processId + '/' + processId + Constants.FINAL_CSV
        sentence_count = 0
        with open(filepath, Constants.CSV_WRITE) as file:
            writer = csv.writer(file)
            for line in data:
                sentence_count = sentence_count + 1
                writer.writerow([line[Constants.SOURCE], line[Constants.TARGET]])

        source_filepath = Constants.BASE_PATH_TOOL_3 + processId + '/' + processId + Constants.SOURCE_TXT
        target_filepath = Constants.BASE_PATH_TOOL_3 + processId + '/' + processId + Constants.TARGET_TXT

        with open(source_filepath, Constants.CSV_WRITE) as source_txt:
            with open(target_filepath, Constants.CSV_WRITE) as target_txt:
                for line in data:
                    source_txt.write(line[Constants.SOURCE] + '\n')
                    target_txt.write(line[Constants.TARGET] + '\n')
            target_txt.close()
            source_txt.close()

        data = {Constants.PATH: Constants.WRITE_TO_FILE,
                Constants.DATA: {
                    Constants.STATUS: Constants.SUCCESS,
                    Constants.PROCESS_ID: processId,
                    Constants.FILES: processId + Constants.FINAL_CSV,
                    Constants.SOURCE_FILE: processId + Constants.SOURCE_TXT,
                    Constants.TARGET_FILE: processId + Constants.TARGET_TXT,
                    Constants.SENTENCE_COUNT: sentence_count
                }}
        send_to_kafka(Constants.EXTRACTOR_RESPONSE, data)
        end_time = get_current_time()
        total_time = end_time - start_time
        log.info('write_to_file : ended at == ' + str(end_time) + ', Total time elapsed == ' + str(total_time))

    except Exception as e:
        log.error('write_to_file : Error occurred while fetching sentences and writing them to file, Error is == '
                  + str(e))
        end_time = get_current_time()
        total_time = end_time - start_time
        data = {Constants.PATH: Constants.SEARCH_REPLACE,
                Constants.DATA: {
                    Constants.STATUS: Constants.FAILED,
                    Constants.PROCESS_ID: processId
                }}
        send_to_kafka(Constants.ERROR_TOPIC, data)
        log.info('write_to_file : ended at == ' + str(end_time) + ', Total time elapsed == ' + str(total_time))
