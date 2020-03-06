from utils.anuvaad_tools_logger import getLogger
from utils.timeutils import get_current_time
from utils.config_reader import read_config_file
import utils.anuvaad_constants as Constants
from kafka_utils.producer import send_to_kafka
import csv
import hashlib
from mongo_utils.sentence_pair import SentencePair
from mongo_utils.sentence import Sentence
from mongo_utils.corpus import Corpus
import datetime
from mongoengine.queryset.visitor import Q
from utils.file_util import write_to_csv
from mongo_utils.mongo_utils import create_sentence_entry_for_translator
from elastic_utils.es_utils import update
from utils.project_utils import get_index, get_hash, contains_english_characters, get_lang

log = getLogger()


def start_search_replace(processId, workspace, configFilePath, selected_files, username,
                         source_language, target_language):
    start_time = get_current_time()
    log.info('start_search_replace : started at ' + str(start_time))
    configFilePath = Constants.BASE_PATH_TOOL_3 + processId + '/' + configFilePath
    config = read_config_file(configFilePath)
    search_replaces = config[Constants.SEARCH_REPLACE]

    file_count = 1
    sentences_matched_count = 0
    try:
        for file in selected_files:
            all_not_selected = list()
            count, not_selected = process(search_replaces, processId, workspace, config, file, file_count)
            sentences_matched_count = sentences_matched_count + count
            all_not_selected = all_not_selected.__add__(not_selected)
            file_count = file_count + 1
        msg_ = {Constants.PATH: Constants.SEARCH_REPLACE,
                Constants.DATA: {
                    Constants.STATUS: Constants.SUCCESS,
                    Constants.PROCESS_ID: processId,
                    Constants.SENTENCE_COUNT: sentences_matched_count
                }}
        log.info('start_search_replace : sentence matched count == ' + str(sentences_matched_count))

        if sentences_matched_count == 0:
            msg = {Constants.PATH: Constants.WRITE_TO_FILE,
                   Constants.DATA: {
                       Constants.SESSION_ID: processId,
                       Constants.USERNAME: username,
                       Constants.TITLE: workspace,
                       Constants.TARGET_LANGUAGE: target_language,
                       Constants.SOURCE_LANGUAGE: source_language
                   }}
            send_to_kafka(Constants.TOPIC_SEARCH_REPLACE, msg)
        else:
            send_to_kafka(Constants.EXTRACTOR_RESPONSE, msg_)
        file_path_not_selected = Constants.BASE_PATH_TOOL_3 + processId + '/' + processId + Constants.NOT_SELECTED_CSV
        write_to_csv(file_path_not_selected, all_not_selected)

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


def get_sentences_count(lines):
    count = 0
    for line in lines:
        count = count + 1
    return count


def process(search_replaces, processId, workspace, config, file, file_count):
    start_time = get_current_time()
    log.info('process : started at ' + str(start_time))
    lines = readfile(processId, file)
    total_sentences = get_sentences_count(lines)
    if total_sentences > 275000:
        raise Exception('more than 275000 lines')
    line_count = 1
    sentence_matched = 0
    not_matched = list()
    for line in lines:
        source = line['source']
        target = line['target']

        new_target = target
        matched = False
        for search_replace in search_replaces:
            eng_text = search_replace[Constants.ENGLISH]
            translated_texts = search_replace[Constants.TRANSLATED]
            replace_text = search_replace[Constants.REPLACE]
            if source.find(eng_text) > -1:

                changes = list()
                for translated_text in translated_texts:
                    if new_target.find(translated_text) > -1:
                        if not matched:
                            sentence_matched = sentence_matched + 1
                            matched = True
                        new_target = new_target.replace(translated_text, replace_text)

                        data = {'source_search': eng_text, 'target_search': translated_text, 'replace': replace_text}
                        changes.append(data)
                        hash_ = get_hash(source)
                        sentences = SentencePair.objects(processId=processId, hash_=hash_)
                        length = len(sentences)

                        if length == 0:
                            create_entry(processId, changes, new_target, source, target, 1, True, hash_)
                        else:
                            SentencePair.objects(processId=processId, hash=hash_).update(is_alone=False)
                            create_entry(processId, changes, new_target, source, target, length + 1, False, hash_)
                        break
                if len(changes) == 0:
                    data = {Constants.SOURCE: source, Constants.TARGET: target}
                    not_matched.append(data)
                changes.clear()
            else:
                data = {Constants.SOURCE: source, Constants.TARGET: target}
                not_matched.append(data)

        line_count = line_count + 1
        log.info('process : line count  == ' + str(line_count))
    end_time = get_current_time()
    total_time = end_time - start_time
    log.info('process : ended at == ' + str(end_time) + ', Total time elapsed == ' + str(total_time))
    return sentence_matched, not_matched


def create_entry(processId, changes, target_update, source, target, serial_no, is_alone, hash_):
    sen = SentencePair(processId=processId, changes=changes, updated=target_update, accepted=False,
                       source=source, target=target, serial_no=serial_no, in_review=False,
                       review_completed=False, is_alone=is_alone, hash=hash_, is_written=False)
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
            file.close()
            return data
    except Exception as e:
        log.info('readfile : error occurred while reading file, Error is == ' + str(e))


def write_to_file(processId, username, workspace, target_language, source_Language):
    start_time = get_current_time()
    log.info('write_to_file : started at ' + str(start_time))
    try:
        target_language = get_lang(target_language)
        source_Language = get_lang(source_Language)
        sentences = SentencePair.objects(processId=processId, accepted=True, is_written=False).limit(400)
        data = list()
        base_path = Constants.BASE_PATH_TOOL_3 + processId + '/' + processId
        filepath_1 = base_path + Constants.FINAL_CSV
        filepath = base_path + '_' + Constants.FINAL_CSV
        source_filepath = base_path + '_english' + Constants.SOURCE_TXT
        target_filepath = base_path + '_' + target_language + Constants.TARGET_TXT
        sentence_count = 0
        unique = set()
        log.info('write_to_file : writing matched sentences started')
        while len(sentences) > 0:
            data = get_all_sentences(sentences)
            update_is_written(sentences)
            write_to_csv(filepath, data)
            sentences = SentencePair.objects(processId=processId, accepted=True, is_written=False).limit(400)

            with open(source_filepath, Constants.CSV_APPEND, encoding='utf-8', errors="ignore") as source_txt:
                with open(target_filepath, Constants.CSV_APPEND, encoding='utf-8', errors="ignore") as target_txt:
                    for line in data:
                        if not unique.__contains__(line[Constants.SOURCE]):
                            sentence_count = sentence_count + 1
                            source_txt.write(line[Constants.SOURCE] + '\n')
                            target_txt.write(line[Constants.TARGET] + '\n')
                            unique.add(line[Constants.SOURCE])
                target_txt.close()
                source_txt.close()
            data.clear()
        log.info('write_to_file : writing matched sentences completed')
        with open(source_filepath, Constants.CSV_APPEND, encoding='utf-8', errors="ignore") as source_txt:
            with open(target_filepath, Constants.CSV_APPEND, encoding='utf-8', errors="ignore") as target_txt:
                not_match_file = Constants.BASE_PATH_TOOL_3 + processId + '/' + processId + Constants.NOT_SELECTED_CSV
                with open(not_match_file, Constants.CSV_RT, encoding='utf-8', errors="ignore") as not_matched:
                    reader = csv.reader(not_matched)

                    with open(filepath, Constants.CSV_APPEND, encoding='utf-8', errors="ignore") as final_csv:
                        final_csv_writer = csv.writer(final_csv)
                        for line in reader:
                            if not unique.__contains__(line[0]):
                                sentence_count = sentence_count + 1
                                source_txt.write(line[0] + '\n')
                                target_txt.write(line[1] + '\n')
                                final_csv_writer.writerow([line[0], line[1]])
                                unique.add(line[0])
                        final_csv.close()
                    unique.clear()
                    not_matched.close()
            target_txt.close()
            source_txt.close()
        log.info('write_to_file : writing not matched sentences completed')
        final_unique = set()

        with open(filepath, Constants.CSV_RT, encoding='utf-8', errors="ignore") as final_csv:
            reader = csv.reader(final_csv)
            with open(filepath_1, Constants.CSV_WRITE, encoding='utf-8', errors="ignore") as final_csv_:
                writer = csv.writer(final_csv_)

                for line in reader:
                    if not final_unique.__contains__(line[0]):
                        final_unique.add(line[0])
                        writer.writerow([line[0], line[1]])
                final_csv_.close()
            final_csv.close()
        log.info('write_to_file : writing to final csv file completed')
        sentences = SentencePair.objects(processId=processId, accepted=False)
        data.clear()
        data = get_all_sentences(sentences)
        x = datetime.datetime.now()
        created_on = str(x.month) + '/' + str(x.day) + '/' + str(x.year) + ', ' + x.strftime("%X")
        if len(data) > 0:
            corp = Corpus(basename=processId, no_of_sentences=len(data), created_on=created_on,
                          last_modified=created_on, author=username, status=Constants.IN_PROGRESS,
                          domain=Constants.DOMAIN_LC, name=workspace, type=Constants.TOOL_CHAIN,
                          source_lang=source_Language, target_lang=target_language)
            corp.save()
            create_sentence_entry_for_translator(processId, data)
        filepath = base_path + '_' + Constants.REJECTED + Constants.FINAL_CSV
        sentence_count_rejected = write_to_csv(filepath, data)

        msg = {Constants.PATH: Constants.WRITE_TO_FILE,
               Constants.DATA: {
                   Constants.STATUS: Constants.SUCCESS,
                   Constants.PROCESS_ID: processId,
                   Constants.SESSION_ID: processId,
                   Constants.FILES: processId + Constants.FINAL_CSV,
                   Constants.SOURCE_FILE: processId + '_english' + Constants.SOURCE_TXT,
                   Constants.TARGET_FILE: processId + '_' + target_language + Constants.TARGET_TXT,
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
        msg = {Constants.PATH: Constants.ERROR_TOPIC,
               Constants.DATA: {
                   Constants.STATUS: Constants.FAILED,
                   Constants.PROCESS_ID: processId
               }}
        send_to_kafka(Constants.ERROR_TOPIC, msg)
        log.info('write_to_file : ended at == ' + str(end_time) + ', Total time elapsed == ' + str(total_time))


def update_is_written(sentences):
    for sentence in sentences:
        SentencePair.objects(processId=sentence['processId'], hash=sentence['hash']).update(is_written=True)


def get_all_sentences(sentences):
    data = list()
    for sentence in sentences:
        source = sentence[Constants.SOURCE]

        res = {Constants.SOURCE: source, Constants.TARGET: sentence[Constants.TARGET],
               Constants.HASH: sentence[Constants.HASH]}
        data.append(res)

    return data


def write_human_processed_corpus(processId):
    log.info('write_human_processed_corpus : started for processId == ' + str(processId))
    try:
        base_path = Constants.BASE_PATH_TOOL_3 + processId + '/'
        filename = processId + '_' + Constants.HUMAN_CORRECTION + Constants.CSV_EXT
        filepath = base_path + filename
        sentence_count = Sentence.objects(basename=processId, status=Constants.ACCEPTED).count()
        corpus = Corpus.objects(basename=processId)
        target_language = corpus[0]['target_lang']
        index = get_index(target_language)
        count = 0
        all_sentences = list()
        hashs_set = list()
        all_hashes = list()
        while count < sentence_count:
            sentences = Sentence.objects(Q(basename=processId, status=Constants.ACCEPTED, completed=False) |
                                         Q(basename=processId, status=Constants.ACCEPTED, completed=None)).limit(100)
            for sentence in sentences:
                if not hashs_set.__contains__(sentence[Constants.HASH]):
                    data = {Constants.SOURCE: sentence[Constants.SOURCE], Constants.TARGET: sentence[Constants.TARGET]}
                    es_data = {Constants.SOURCE: sentence[Constants.SOURCE],
                               Constants.TARGET: sentence[Constants.TARGET],
                               Constants.IS_TRANSLATION_COMPLETED: contains_english_characters(
                                   sentence[Constants.TARGET])
                               }
                    update(sentence[Constants.HASH], index, es_data)
                    all_sentences.append(data)
                    hashs_set.append(sentence[Constants.HASH])
                    count = count + len(hashs_set)
            Sentence.objects(basename=hashs_set).update(completed=True)
            all_hashes = all_hashes.__add__(hashs_set)
            hashs_set.clear()
        total_sentences_after_write = write_to_csv(filepath, all_sentences, mode=Constants.CSV_WRITE)
        Sentence.objects(basename=all_hashes).update(completed=False)

        msg = {Constants.PATH: Constants.HUMAN_CORRECTION,
               Constants.DATA: {
                   Constants.STATUS: Constants.SUCCESS,
                   Constants.PROCESS_ID: processId,
                   Constants.SESSION_ID: processId,
                   Constants.FILES: filename,
                   Constants.SENTENCE_COUNT: total_sentences_after_write
               }}

        send_to_kafka(Constants.EXTRACTOR_RESPONSE, msg)
        log.info('write_human_processed_corpus : ended for processId == ' + str(processId))
    except Exception as e:
        log.error('write_human_processed_corpus : error occurred, Error is == ' + str(e))
        msg = {Constants.PATH: Constants.ERROR_TOPIC,
               Constants.DATA: {
                   Constants.STATUS: Constants.FAILED,
                   Constants.PROCESS_ID: processId,
                   Constants.TYPE: Constants.HUMAN_CORRECTION
               }}
        send_to_kafka(Constants.ERROR_TOPIC, msg)
