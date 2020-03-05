from utils.anuvaad_tools_logger import getLogger
from utils.timeutils import get_current_time
from utils.config_reader import read_config_file
from sentence_extractor.custom_nltk_tokenizer import get_tokenizer_english_pickle, update_english_pickle_with_tokens
from sentence_extractor.utils import write_to_csv
from kafka_utils.producer import send_to_kafka
import utils.anuvaad_constants as Constants
from utils.file_util import read_csv
import csv
import sys

maxInt = sys.maxsize
csv.field_size_limit(maxInt)

log = getLogger()


def start_sentence_extraction(configFilePath, posTokenFilePath, negTokenFilePath, paragraphFilePath, processId,
                              workspace, message):
    try:
        start_time = get_current_time()
        log.info('start_sentence_extraction : started at == ' + str(start_time))
        config = read_config_file(Constants.BASE_PATH_TOOL_1 + processId + "/" + configFilePath)
        tokens = load_tokens(config, posTokenFilePath, negTokenFilePath, processId)
        sentence_end_characters = config[Constants.SEC]
        specific_file_header = config[Constants.SFILE_HEADER]
        tokenizer = load_tokenizer(tokens, config, sentence_end_characters)
        paragraphs = read_csv(Constants.BASE_PATH_TOOL_1 + processId + "/" + paragraphFilePath)
        log.info('start_sentence_extraction : paragraphs found == ' + str(len(paragraphs)))
        sentences = extract_sentences_from_paragraphs(tokenizer, paragraphs)
        log.info('start_sentence_extraction : sentences found == ' + str(len(sentences)))
        all_unique_sentences = remove_duplicates(sentences)  ##set
        all_unique_sentences = apply_sentence_len_rule(all_unique_sentences, config)
        log.info('start_sentence_extraction : unique sentences found == ' + str(len(all_unique_sentences)))
        filename = write_to_csv(all_unique_sentences, processId, specific_file_header + '_' + Constants.SENTENCES,
                                Constants.BASE_PATH_TOOL_1, workspace)
        res = {'path': 'sentences',
               'data': {
                   'processId': processId,
                   'sentencesFile': filename,
                   'sentencesCount': len(all_unique_sentences)
               }}
        try:
            log.info('start_sentence_extraction : trying to send message to queue after sentences extraction')
            log.info('start_sentence_extraction : message == ' + str(res))

            send_to_kafka(topic=Constants.EXTRACTOR_RESPONSE, value=res)
            log.info('start_sentence_extraction : message sent to queue after sentences extraction')
        except Exception as e:
            log.error('start_sentence_extraction : Error coccured while sending the message to topic == ' +
                      str(Constants.EXTRACTOR_RESPONSE) + ' with ERROR == ' + str(e))

            send_to_kafka(topic=Constants.ERROR_TOPIC, value=message)

        end_time = get_current_time()
        log.info('start_sentence_extraction : ended at == ' + str(end_time))
        total_time = end_time - start_time
        log.info('start_sentence_extraction : total time elapsed == ' + str(total_time) + ' for proceessId == '
                 + str(processId))
    except Exception as e:
        log.error('start_sentence_extraction : Error coccured while sending the message to topic == ' +
                  str(Constants.EXTRACTOR_RESPONSE) + ' with ERROR == ' + str(e))

        send_to_kafka(topic=Constants.ERROR_TOPIC, value=message)


def apply_sentence_len_rule(sentences, config):
    filtered = list()
    min_length = config[Constants.MIN_SEN_LENGTH]
    for sentence in sentences:
        if len(sentence) > min_length:
            filtered.append(sentence)
    return filtered


def remove_duplicates(sentences):
    unique = set()
    for sentence in sentences:
        unique.add(sentence)
    return unique


def extract_sentences_from_paragraphs(tokenizer, paragraphs):
    start_time = get_current_time()
    log.info('extract_sentences_from_paragraphs :  started at = ' + str(start_time))
    all_sentences = []
    line_in_file = 0
    log.info('extract_sentences_from_paragraphs : contains total line == ' + str(len(paragraphs)))
    for text in paragraphs:
        if text != '':
            line_in_file = line_in_file + 1
            text = preprocess_paragraph(text)
            log.info('extract_sentences_from_paragraphs :  line number processing in file == ' + str(line_in_file)
                     + ' out of ' + str(len(paragraphs)))
            sentences = tokenizer.tokenize(text)
            log.info('extract_sentences_from_paragraphs :  sentences found ' + str(len(sentences)))
            rejected_count = 0
            for sentence in sentences:
                if len(sentence) < 1200:
                    all_sentences.append(sentence)
                else:
                    rejected_count = rejected_count + 1
            log.info('extract_sentences_from_paragraphs :  rejected sentence count ' + str(rejected_count))
    end_time = get_current_time()
    log.info('extract_sentences_from_paragraphs :  total sentences found ' + str(len(all_sentences)))
    log.info('extract_sentences_from_paragraphs :  ended at = ' + str(end_time))
    return all_sentences


def preprocess_paragraph(text):
    text = text.replace(' No. ', ' No.')
    text = text.replace(' no. ', ' no.')
    text = text.replace(' NO. ', ' NO.')
    text = text.replace('“', ' ')
    text = text.replace('”', ' ')
    return text


def load_tokenizer(tokens, config, sentence_end_characters):
    log.info('load_tokenizer : started ')
    tokenizer = get_tokenizer_english_pickle()
    exclusive_tokens = load_exclusive_tokens(config)
    tokenizer = update_english_pickle_with_tokens(tokenizer, tokens, exclusive_tokens)
    log.info('load_tokenizer : ended ')
    return tokenizer


def load_exclusive_tokens(config):
    exclusive_tokens = config[Constants.EXCLUSIVE_TOKENS]
    num = 50
    i = 0
    while i < num:
        exclusive_tokens.append(' ' + str(i))
        i = i + 1

    return exclusive_tokens


def load_tokens(config, posTokenFilePath, negTokenFilePath, processId):
    start_time = get_current_time()
    log.info('load_tokens : started at ' + str(start_time))
    positiveTokens = read_csv(Constants.BASE_PATH_TOOL_1 + processId + "/" + posTokenFilePath)
    negativeTokens = read_csv(Constants.BASE_PATH_TOOL_1 + processId + "/" + negTokenFilePath)
    tokens = [x for x in positiveTokens if x not in negativeTokens]
    end_time = get_current_time()
    log.info('load_tokens : ended at ' + str(end_time))
    return tokens
