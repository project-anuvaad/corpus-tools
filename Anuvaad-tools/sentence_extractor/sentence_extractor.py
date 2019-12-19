from utils.anuvaad_tools_logger import getLogger
from utils.timeutils import get_current_time
from sentence_extractor.extractor_config_reader import read_config_file
from sentence_extractor.custom_nltk_tokenizer import get_tokenizer_english_pickle,update_english_pickle_with_tokens
from sentence_extractor.utils import write_to_csv
from kafka_utils.producer import get_producer
import sentence_extractor.extractor_constants as Constants
import csv
from  sentence_extractor import anuvaad_exceptions  as EXP
log = getLogger()


def start_sentence_extraction(configFilePath, posTokenFilePath, negTokenFilePath, paragraphFilePath, processId):
    start_time = get_current_time()
    log.info('start_sentence_extraction : started at == ' + str(start_time))
    config = read_config_file(Constants.BASE_PATH_TOOL_1 + processId + "/" + configFilePath)
    tokens = load_tokens(config, posTokenFilePath, negTokenFilePath, processId)
    sentence_end_characters = config[Constants.SEC]
    specific_file_header = config[Constants.SFILE_HEADER]
    tokenizer = load_tokenizer(tokens, sentence_end_characters)
    paragraphs = read_data_from_csv(Constants.BASE_PATH_TOOL_1 + processId + "/" + paragraphFilePath)
    log.info('start_sentence_extraction : paragraphs found == ' + str(len(paragraphs)))
    sentences = extract_sentences_from_paragraphs(tokenizer, paragraphs)
    log.info('start_sentence_extraction : sentences found == ' + str(len(sentences)))
    all_unique_sentences = remove_duplicates(sentences)  ##set
    log.info('start_sentence_extraction : unique sentences found == ' + str(len(all_unique_sentences)))
    filename = write_to_csv(all_unique_sentences, processId, specific_file_header, Constants.BASE_PATH_TOOL_1)
    res = {'path': 'sentences',
           'data': {
               'processId': processId,
               'sentencesFile': filename,
               'sentencesCount': len(all_unique_sentences)
           }}
    try:
        log.info('start_sentence_extraction : trying to send message to queue after sentences extraction')
        log.info('start_sentence_extraction : message == ' + str(res))
        producer = get_producer()
        producer.send(topic=Constants.EXTRACTOR_RESPONSE, value=res)
        producer.flush()
        log.info('start_sentence_extraction : message sent to queue after sentences extraction')
    except Exception as e:
        log.error('start_sentence_extraction : Error coccured while sending the message to topic == ' +
                  str(Constants.EXTRACTOR_RESPONSE) + ' with ERROR == ' + str(e))


    end_time = get_current_time()
    log.info('start_sentence_extraction : ended at == ' + str(end_time))
    total_time = end_time - start_time
    log.info('start_sentence_extraction : total time elapsed == '+str(total_time) + ' for proceessId == '
             + str(processId))


def remove_duplicates(sentences):
    unique = set()
    for sentence in sentences:
        unique.add(sentence)
    return unique


def extract_sentences_from_paragraphs(tokenizer, paragraphs):
    start_time = get_current_time()
    log.info('extract_sentences_from_paragraphs :  started at = ' + str(start_time))
    all_sentences = []
    for text in paragraphs:
        log.info('extract_sentences_from_paragraphs :  text found ' + text)
        if text == '':
            return all_sentences
        sentences = tokenizer.tokenize(text)
        log.info('extract_sentences_from_paragraphs :  sentences found ' + str(len(sentences)))
        all_sentences.__add__(sentences)
    end_time = get_current_time()
    log.info('extract_sentences_from_paragraphs :  ended at = ' + str(end_time))
    return all_sentences


def load_tokenizer(tokens, sentence_end_characters):
    tokenizer = get_tokenizer_english_pickle()
    tokenizer = update_english_pickle_with_tokens(tokenizer, tokens)
    return tokenizer


def load_tokens(config, posTokenFilePath, negTokenFilePath, processId):
    start_time = get_current_time()
    log.info('load_tokens : started at '+str(start_time))
    positiveTokens = read_data_from_csv(Constants.BASE_PATH_TOOL_1 + processId + "/" + posTokenFilePath)
    negativeTokens = read_data_from_csv(Constants.BASE_PATH_TOOL_1 + processId + "/" + negTokenFilePath)
    tokens = [x for x in positiveTokens if x not in negativeTokens]
    end_time = get_current_time()
    log.info('load_tokens : ended at ' + str(end_time))
    return tokens


def read_data_from_csv(filePath):
    tokens = []
    with open(filePath, 'rt') as file:
        data = csv.reader(filePath)
        for row in data:
            text = row[0]
            tokens.append(text)
    file.close()
    return tokens
