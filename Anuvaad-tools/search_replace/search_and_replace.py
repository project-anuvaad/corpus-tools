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
    sentences_matched_count = 0
    try:
        for file in selected_files:
            sentences_matched_count = sentences_matched_count + \
                                      process(search_replaces, processId, workspace, config, file, file_count)
            file_count = file_count + 1
        msg = {Constants.PATH: Constants.SEARCH_REPLACE,
               Constants.DATA: {
                   Constants.STATUS: Constants.SUCCESS,
                   Constants.PROCESS_ID: processId,
                   Constants.SENTENCE_COUNT: sentences_matched_count
               }}
        send_to_kafka(Constants.EXTRACTOR_RESPONSE, msg)
    except Exception as e:
        log.error('start_search_replace : Error occurred while processing files, Error is ==  ' + str(e))
        msg = {Constants.PATH: Constants.SEARCH_REPLACE,
               Constants.DATA: {
                   Constants.STATUS: Constants.FAILED,
                   Constants.PROCESS_ID: processId,
               }}
        send_to_kafka(Constants.ERROR_TOPIC, msg)
    end_time = get_current_time()
    total_time = end_time - start_time
    log.info('start_search_replace : ended at == ' + str(end_time) + ', Total time elapsed == ' + str(total_time))


def process(search_replaces, processId, workspace, config, file, file_count):
    start_time = get_current_time()
    log.info('process : started at ' + str(start_time))
    lines = readfile(processId, file)
    line_count = 1
    sentence_matched = 0
    for line in lines:
        source = line['source']
        target = line['target']
        changes = list()
        new_target = target
        matched = False
        for search_replace in search_replaces:
            eng_text = search_replace[Constants.ENGLISH]
            translated_text = search_replace[Constants.TRANSLATED]
            replace_text = search_replace[Constants.REPLACE]
            if source.find(eng_text) > -1 and target.find(translated_text) > -1:
                if not matched:
                    sentence_matched = sentence_matched + 1
                    matched = True
                new_target = new_target.replace(translated_text, replace_text)
                data = {'source_search': eng_text, 'target_search': translated_text, 'replace': replace_text}
                changes.append(data)
        serial_no = file_count * 1000 + line_count
        if len(changes) > 0:
            create_entry(processId, changes, new_target, source, target, serial_no)
        else:
            sen = SentencePairUnchecked(processId=processId, source=source, target=target, serial_no=serial_no)
            sen.save()
        line_count = line_count + 1

    end_time = get_current_time()
    total_time = end_time - start_time
    log.info('process : ended at == ' + str(end_time) + ', Total time elapsed == ' + str(total_time))
    return sentence_matched


def create_entry(processId, changes, target_update, source, target, serial_no):
    sen = SentencePair(processId=processId, changes=changes, updated=target_update, accepted=False,
                       source=source, target=target, serial_no=serial_no, in_review=False, review_completed=False)
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
        data = get_all_sentences(sentences)
        sentences = SentencePair.objects(processId=processId, accepted=True)
        data = data.__add__(get_all_sentences(sentences))
        base_path = Constants.BASE_PATH_TOOL_3 + processId + '/' + processId
        filepath = base_path + Constants.FINAL_CSV
        sentence_count = write_to_csv(filepath, data)
        source_filepath = base_path + Constants.SOURCE_TXT
        target_filepath = base_path + Constants.TARGET_TXT

        with open(source_filepath, Constants.CSV_WRITE) as source_txt:
            with open(target_filepath, Constants.CSV_WRITE) as target_txt:
                for line in data:
                    source_txt.write(line[Constants.SOURCE] + '\n')
                    target_txt.write(line[Constants.TARGET] + '\n')
            target_txt.close()
            source_txt.close()

        sentences = SentencePair.objects(processId=processId, accepted=False)
        data.clear()
        data = get_all_sentences(sentences)
        filepath = base_path + '_' + Constants.REJECTED + Constants.FINAL_CSV
        sentence_count_rejected = write_to_csv(filepath, data)

        msg = {Constants.PATH: Constants.WRITE_TO_FILE,
               Constants.DATA: {
                   Constants.STATUS: Constants.SUCCESS,
                   Constants.PROCESS_ID: processId,
                   Constants.FILES: processId + Constants.FINAL_CSV,
                   Constants.SOURCE_FILE: processId + Constants.SOURCE_TXT,
                   Constants.TARGET_FILE: processId + Constants.TARGET_TXT,
                   Constants.SENTENCE_COUNT: sentence_count,
                   Constants.SENTENCE_COUNT_REJECTED: sentence_count_rejected,
                   Constants.REJECTED_FILE: processId + '_' + Constants.REJECTED + Constants.FINAL_CSV
               }}

        send_to_kafka(Constants.EXTRACTOR_RESPONSE, msg)
        end_time = get_current_time()
        total_time = end_time - start_time
        log.info('write_to_file : ended at == ' + str(end_time) + ', Total time elapsed == ' + str(total_time))

    except Exception as e:
        log.error('write_to_file : Error occurred while fetching sentences and writing them to file, Error is == '
                  + str(e))
        end_time = get_current_time()
        total_time = end_time - start_time
        msg = {Constants.PATH: Constants.SEARCH_REPLACE,
               Constants.DATA: {
                   Constants.STATUS: Constants.FAILED,
                   Constants.PROCESS_ID: processId
               }}
        send_to_kafka(Constants.ERROR_TOPIC, msg)
        log.info('write_to_file : ended at == ' + str(end_time) + ', Total time elapsed == ' + str(total_time))


def write_to_csv(filepath, data):
    sentence_count = 0
    with open(filepath, Constants.CSV_WRITE) as file:
        writer = csv.writer(file)
        for line in data:
            sentence_count = sentence_count + 1
            writer.writerow([line[Constants.SOURCE], line[Constants.TARGET]])
    return sentence_count


def get_all_sentences(sentences):
    data = list()
    for sentence in sentences:
        res = {Constants.SOURCE: sentence[Constants.SOURCE], Constants.TARGET: sentence[Constants.TARGET]}
        data.append(res)
    return data
