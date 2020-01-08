from utils.anuvaad_tools_logger import getLogger
import utils.anuvaad_constants as Constants
from kafka_utils.producer import send_to_kafka
from utils.timeutils import get_current_time, get_date_time
import time
from elastic_utils.es_utils import create_sentence, update, get_all_by_ids, create

import hashlib
import csv

log = getLogger()


def start_machine_translation(processId, workspace, sourceFiles, targetLanguage, created_by=None,
                              update_by=None, domain=None, use_latest=False):
    start_time = get_current_time()
    log.info('start_machine_translation : for processId' + str(processId) + ' started at == ' + str(start_time))
    try:
        validation = check_file_validity(processId, sourceFiles)
        if validation[Constants.STATUS]:

            for file in sourceFiles:
                file_count = len(sourceFiles)
                basepath = Constants.BASE_PATH_MT + processId + '/'
                filename = basepath + file
                data_ = {
                    Constants.PROCESS_ID: processId,
                    Constants.FILE_COUNT: file_count,
                    Constants.COMPLETE: 0,
                    Constants.PATH: basepath,
                    Constants.FILES: sourceFiles
                }

                create(data_, Constants.FILE_INDEX)

                process_file(filename, processId, workspace, targetLanguage, created_by,
                             update_by, domain, use_latest)
                index = get_index(targetLanguage)

                message = {
                    Constants.TYPE: Constants.WRITE_CSV,
                    Constants.FILE_NAME: filename,
                    Constants.INDEX: index,
                    Constants.PROCESS_ID: processId
                }
                data = {Constants.DATA: message}
                send_to_kafka(topic=Constants.TOPIC_FETCHER, value=data)

        else:
            res = {'path': 'mt',
                   'data': {
                       'status': False,
                       'processId': processId,
                       'files': validation[Constants.FILES],
                   }}
            log.info('start_machine_translation : validation for the following files failed : ' +
                     str(validation[Constants.FILES]))

            send_to_kafka(topic=Constants.ERROR_TOPIC, value=res)

    except Exception as e:
        log.error('start_machine_translation : Error occurred for processId '
                  + str(processId) + ', error is == ' + str(e))


def check_file_validity(processId, sourceFiles):
    base_path = Constants.BASE_PATH_MT + processId
    wrong_file = []
    log.info('check_file_validity : checking for files in directory ')
    for file in sourceFiles:
        filepath = base_path + '/' + file
        try:
            log.info('check_file_validity : checking for file == ' + str(filepath))
            with open(filepath, Constants.CSV_RT) as test:
                test.close()
        except Exception as e:
            log.error('check_file_validity : ERROR Occurred while opening file == ' + str(filepath) +
                      ' Error is == ' + str(e))
            wrong_file.append(filepath)

    if not len(wrong_file) == 0:
        log.info('check_file_validity : following files were not present on disk ' + str(wrong_file))
        return {Constants.STATUS: False, Constants.FILES: wrong_file}
    else:
        return {Constants.STATUS: True, Constants.FILES: wrong_file}


def process_file(filename, processId, workspace, targetLanguage, created_by=None,
                 update_by=None, domain=None, use_latest=False):
    log.info('process_file : started for filename == ' + str(filename) + ', processId == ' + str(processId) +
             ', workspace == ' + str(workspace) + ', target language == ' + str(targetLanguage))
    try:
        all_hashes = []
        with open(filename, Constants.CSV_RT) as file:
            data = csv.reader(file)
            count = 0
            sentences = {}
            hashes = []
            for row in data:
                count = count + 1
                text = row[0]
                encoded_str = hashlib.sha256(text.encode())
                hash_hex = encoded_str.hexdigest()
                date_time = get_date_time()
                all_hashes.append(hash_hex)
                sentence = {
                    Constants.PROCESS_ID: processId,
                    Constants.DOCUMENT_TITLE: workspace,
                    Constants.CREATED_DATE: date_time,
                    Constants.CREATED_BY: created_by,
                    Constants.SOURCE_LANG: Constants.ENGLISH,
                    Constants.TARGET_LANGUAGE: targetLanguage,
                    Constants.UPDATED_DATE: date_time,
                    Constants.UPDATED_BY: update_by,
                    Constants.DOMAIN: domain,
                    Constants.SOURCE_SENTENCE: text,
                    Constants.SENTENCES: [],
                    Constants.IS_COMPLETE: False,
                    Constants.HASH: hash_hex,
                    Constants.FILE_NAME: filename
                }
                hashes.append(hash_hex)
                sentences[hash_hex] = sentence

                if count == Constants.BATCH_SIZE:
                    send_for_processing(sentences, hashes, processId, targetLanguage, filename, use_latest)
                    count = 0
                    sentences.clear()
                    hashes.clear()
            if len(sentences) > 0:
                send_for_processing(sentences, hashes, processId, targetLanguage, filename, use_latest)
            write_to_csv(all_hashes, filename)
    except Exception as e:
        log.error('process_file : Error occurred for filename ==  ' + str(filename) + ', processId == ' + str(processId)
                  + ', Error is == ' + str(e))


def send_for_processing(sentences, hashes, processId, targetLanguage, filename, use_latest):
    try:
        log.info('send_for_processing : sending sentences in next step for processId == ' + str(processId))
        message = {
            Constants.TYPE: Constants.TRANSLATE,
            Constants.SENTENCES: sentences,
            Constants.HASHES: hashes,
            Constants.PROCESS_ID: processId,
            Constants.TARGET_LANGUAGE: targetLanguage,
            Constants.USE_LATEST: use_latest,
            Constants.FILE_NAME: filename
        }
        data = {Constants.DATA: message}
        send_to_kafka(topic=Constants.TOPIC_FETCHER, value=data)
        log.info('send_for_processing : sentences sent to next step with body == ' + str(message))
    except Exception as e:
        log.error('send_for_processing : Error occurred for sending sentences in next step, Error is ==  ' + str(e))


def write_to_csv(data, filename):
    filename = change_csv_filename(filename, '_h')
    with open(filename, Constants.CSV_WRITE) as file:
        writer = csv.writer(file)
        for line in data:
            writer.writerow([line])
        file.close()
    return filename


def check_and_translate(message):
    hashes = message[Constants.HASHES]
    sentences = message[Constants.SENTENCES]
    use_latest = message[Constants.USE_LATEST]
    targetLanguage = message[Constants.TARGET_LANGUAGE]
    filename = message[Constants.FILE_NAME]
    index = get_index(targetLanguage)
    try:
        data = get_all_by_ids(hashes, index)
        log.info('check_and_translate : use_latest == ' + str(use_latest))
        if use_latest:

            for key in data:
                sentence = data[key]
                sentences.pop(key)
                text = [sentence[Constants.SOURCE_SENTENCE]]
                translated_sentences = translate_from_google(text, targetLanguage)
                translated_sentence = translated_sentences[0][Constants.TARGET]
                sentence[Constants.TARGET_SENTENCE] = translated_sentence
                sentence[Constants.UPDATED_DATE] = get_date_time()
                target_sentence = create_target_sentence(sentence, filename)
                sentence[Constants.TARGET_SENTENCES].append(target_sentence)
                update(id_=key, index=index, body=sentence)
            log.info('check_and_translate : now trasnlating')
            translate_and_create_es(sentences, targetLanguage, filename, index)
        else:
            for key in data:
                sentences.pop(key)
            log.info('check_and_translate : now trasnlating 2')
            translate_and_create_es(sentences, targetLanguage, filename, index)

    except Exception as e:
        log.error('check_and_translate : Error occurred, error is == ' + str(e))


def translate_and_create_es(sentences, targetLanguage, filename, index):
    sen_text = []
    for key in sentences:
        sentence = sentences[key]
        sen_text.append(sentence[Constants.SOURCE_SENTENCE])
    if len(sen_text) > 0:
        translated_sentences = translate_from_google(sen_text, targetLanguage)
        for translated_sentence in translated_sentences:
            text = translated_sentence[Constants.SOURCE]
            encoded_str = hashlib.sha256(text.encode())
            hash_hex = encoded_str.hexdigest()
            sentence = sentences[hash_hex]
            sentence[Constants.TARGET_SENTENCE] = translated_sentence[Constants.TARGET]
            log.info('Before create target_sentence')
            target_sentence = create_target_sentence(sentence, filename)
            sentence[Constants.TARGET_SENTENCES] = [target_sentence]
            log.info('Before create')
            create_sentence(sentence, index)


def translate_from_google(data, target):
    from google.cloud import translate
    start_time = get_current_time()
    log.info('translate_from_google : started at == ' + str(start_time))
    log.info('translate_from_google : ' + str(data))
    try:
        translation_list = []
        translate_client = translate.Client()
        translations = translate_client.translate(data, target_language=target)
        for translation in translations:
            log.info('Translated sentence is == ' + str(translation))
            data = {Constants.TARGET: translation['translatedText'], Constants.SOURCE: translation['input']}
            translation_list.append(data)
        return translation_list
    except Exception as e:
        log.error('translate_from_google : Error Occurred while translating using google, error is == ' + str(e))
        log.info('translate_from_google : waiting for some retry time before making api request, time is == '
                 + str(Constants.DEFAULT_WAIT_TIME))
        time.sleep(Constants.DEFAULT_WAIT_TIME)
        return translate_from_google(data, target)


def write_csv_for_translation(message):
    log.info('write_csv_for_translation : started for filename == ' + str(message[Constants.FILE_NAME]))
    filename_t = change_csv_filename(message[Constants.FILE_NAME], '_t')
    filename_h = change_csv_filename(message[Constants.FILE_NAME], '_h')
    index = message[Constants.INDEX]
    try:
        with open(filename_h, Constants.CSV_RT) as file:
            with open(filename_t, Constants.CSV_WRITE) as target:
                writer = csv.writer(target)
                data = csv.reader(file)
                for row in data:
                    text = row[0]
                    es_response = get_all_by_ids([text], index)
                    source = es_response[text]
                    translated_text = source[Constants.TARGET_SENTENCE]
                    source_text = source[Constants.SOURCE_SENTENCE]
                    writer.writerow([source_text, translated_text])
                target.close()
            file.close()
        return filename_t
    except Exception as e:
        log.error('write_csv_for_translation : error occured for fetching and writing translation,'
                  + ' Error is == ' + str(e))


def get_index(target_language):
    index_data = {
        'hi': 'en-hi',
        'bn': 'en-bn',
        'gu': 'en-gu',
        'mr': 'en-mr',
        'kn': 'en-kn',
        'te': 'en-te',
        'ml': 'en-ml',
        'pa': 'en-pa',
        'ta': 'en-ta'
    }
    return index_data[target_language]


def create_target_sentence(sentence, filename):
    log.info('create_target_sentence : started')
    target_sentence = {
        Constants.TARGET_SENTENCE: sentence[Constants.TARGET_SENTENCE],
        Constants.CREATED_DATE: get_date_time(),
        Constants.CREATED_BY: sentence[Constants.CREATED_BY],
        Constants.TARGET_LANGUAGE: sentence[Constants.TARGET_LANGUAGE],
        Constants.FILE_NAME: filename
    }
    log.info('create_target_sentence : ended')
    return target_sentence


def merge_files(filenames, processId, basepath):
    log.info('merge_files : started')
    merged_file_name = basepath + processId + '_merged.csv'

    try:
        with open(merged_file_name, Constants.CSV_WRITE) as merge:
            writer = csv.writer(merge)
            for filename in filenames:
                target_filename = change_csv_filename(filename, '_t')
                target_filename = basepath + target_filename
                with open(target_filename, Constants.CSV_RT) as source:
                    source_reader = csv.reader(source)
                    for data in source_reader:
                        writer.writerow(data)
                source.close()
        merge.close()
        log.info('merge_files : ended')
        return processId + '_merged.csv'

    except Exception as e:
        log.error('merge_files : Error occurred while merging files for process_id == ' + str(processId))
        log.error('merge_files : Error occurred while merging files, Error is == ' + str(e))
        return None


def change_csv_filename(filename, added_name):
    name = filename.split('.csv')
    target_name = name[0] + added_name + '.csv'
    return target_name
