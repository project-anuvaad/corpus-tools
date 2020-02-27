from utils.anuvaad_tools_logger import getLogger
import utils.anuvaad_constants as Constants
from kafka_utils.producer import send_to_kafka
from utils.timeutils import get_current_time, get_date_time
import time
from elastic_utils.es_utils import create_sentence, update, get_all_by_ids, create
from utils.file_util import write_to_csv
from mongo_utils.corpus import Corpus
from utils.project_utils import get_index, contains_english_characters, get_hash
from mongo_utils.mongo_utils import create_sentence_entry_for_translator
import hashlib
import csv
import datetime

log = getLogger()


def start_machine_translation(processId, workspace, sourceFiles, targetLanguage, created_by=None,
                              update_by=None, domain=None, use_latest=False):
    start_time = get_current_time()
    log.info('start_machine_translation : for processId' + str(processId) + ' started at == ' + str(start_time))
    try:
        validation = check_file_validity(processId, sourceFiles)
        if validation[Constants.STATUS]:
            file_write_messages = []
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
                file_write_messages.append(message)
            for message in file_write_messages:
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
                text = row[0]

                if len(text) < 1000 and not contains_english_characters(text):
                    count = count + 1
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
                else:
                    log.info('process_file : Rejecting sentence with length == '
                             + str(len(text)) + ',  text is == ' + str(text))
                if count == Constants.BATCH_SIZE:
                    send_for_processing(sentences, hashes, processId, targetLanguage, filename, use_latest)
                    count = 0
                    sentences.clear()
                    hashes.clear()
            if len(sentences) > 0:
                send_for_processing(sentences, hashes, processId, targetLanguage, filename, use_latest)
            hash_file_name = change_csv_filename(filename, '_h')
            write_to_csv(hash_file_name, all_hashes, rows=1)
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
                sentence[Constants.IS_TRANSLATION_COMPLETED] = contains_english_characters(translated_sentence)
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
            hash_hex = get_hash(text)
            sentence = sentences[hash_hex]
            sentence[Constants.TARGET_SENTENCE] = translated_sentence[Constants.TARGET]
            sentence[Constants.IS_TRANSLATION_COMPLETED] = \
                contains_english_characters(translated_sentence[Constants.TARGET])
            target_sentence = create_target_sentence(sentence, filename)
            sentence[Constants.TARGET_SENTENCES] = [target_sentence]
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
    processId = message[Constants.PROCESS_ID]
    junk_corpus_sentences = list()
    try:
        with open(filename_h, Constants.CSV_RT) as file:
            with open(filename_t, Constants.CSV_WRITE) as target:
                writer = csv.writer(target)
                data = csv.reader(file)
                ids = list()
                for row in data:
                    _id = row[0]
                    ids.append(_id)
                    if len(ids) == 100:
                        es_responses = get_all_by_ids(ids, index)
                        for id_ in ids:
                            source = es_responses[id_]
                            translated_text = source[Constants.TARGET_SENTENCE]
                            is_complete = source[Constants.IS_TRANSLATION_COMPLETED]
                            source_text = source[Constants.SOURCE_SENTENCE]
                            if is_complete is None or is_complete is True:
                                writer.writerow([source_text, translated_text])
                            else:
                                junk_corpus_sentences.append(source)
                        ids.clear()
                if len(ids) > 0:
                    for id_ in ids:
                        source = es_responses[id_]
                        translated_text = source[Constants.TARGET_SENTENCE]
                        source_text = source[Constants.SOURCE_SENTENCE]
                        if is_complete is None or is_complete is True:
                            writer.writerow([source_text, translated_text])
                        else:
                            junk_corpus_sentences.append(source)
                    ids.clear()
                target.close()
            file.close()
            create_corpus_for_translator(junk_corpus_sentences, processId)
        log.info('write_csv_for_translation : ended for filename == ' + str(message[Constants.FILE_NAME]))
        return filename_t
    except Exception as e:
        log.error('write_csv_for_translation : error occurred for fetching and writing translation,'
                  + ' Error is == ' + str(e))


def create_corpus_for_translator(sentences, processId):
    log.info('create_corpus_for_translator : started for processId == ' + str(processId))
    corpus = Corpus.objects(basename=processId)
    if len(corpus) == 0:
        x = datetime.datetime.now()
        created_on = str(x.month) + '/' + str(x.day) + '/' + str(x.year) + ', ' + x.strftime("%X")
        sentence = sentences[0]
        corpus = Corpus(basename=processId, no_of_sentences=len(sentences), created_on=created_on,
                        last_modified=created_on, status=Constants.IN_PROGRESS,
                        domain=Constants.DOMAIN_LC, type=Constants.TOOL_CHAIN,
                        source_lang=sentence[Constants.SOURCE_LANGUAGE],
                        target_lang=sentence[Constants.TARGET_LANGUAGE])
        corpus.save()
    else:
        corpus = corpus[0]
        length = corpus[Constants.NO_OF_SENTENCES] + len(sentences)
        Corpus.objects(basename=processId).update(no_of_sentences=length)

    create_sentence_entry_for_translator(processId, sentences)
    log.info('create_corpus_for_translator : ended for processId == ' + str(processId))


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
    no_of_sentences = 0
    unique = set()
    try:
        with open(merged_file_name, Constants.CSV_WRITE) as merge:
            writer = csv.writer(merge)
            for filename in filenames:
                target_filename = change_csv_filename(filename, '_t')
                target_filename = basepath + target_filename
                with open(target_filename, Constants.CSV_RT) as source:
                    source_reader = csv.reader(source)
                    for data in source_reader:
                        if not unique.__contains__(data[0]):
                            no_of_sentences = no_of_sentences + 1
                            unique.add(data[0])
                            writer.writerow(data)
                source.close()
        merge.close()
        log.info('merge_files : ended')
        return processId + '_merged.csv', no_of_sentences

    except Exception as e:
        log.error('merge_files : Error occurred while merging files for process_id == ' + str(processId))
        log.error('merge_files : Error occurred while merging files, Error is == ' + str(e))
        return None


def change_csv_filename(filename, added_name):
    name = filename.split('.csv')
    target_name = name[0] + added_name + '.csv'
    return target_name


def check_elastic_for_new_sentences(hashs, index):
    count = 0
    already_present = 0
    hash_list = list()
    for hash_ in hashs:
        hash_list.append(hash_)
        count = count + 1
        if count == 100:
            data = get_all_by_ids(hash_list, index)
            already_present = already_present + len(data.keys())
            count = 0
            hash_list.clear()
    if len(hash_list) > 0:
        data = get_all_by_ids(hash_list, index)
        already_present = already_present + len(data.keys())
        hash_list.clear()
    return already_present
