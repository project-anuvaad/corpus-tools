import sentence_extractor.extractor_constants as Constants
import sentence_extractor.extractor_config_reader as Config_reader
from utils.anuvaad_tools_logger import getLogger
from utils.timeutils import get_current_time
from kafka_utils.producer import get_producer
from sentence_extractor.utils import write_to_csv
import sentence_extractor.extractor_constants as Constants
import sentence_extractor.utils as Utils
import csv
import sys
import re

maxInt = sys.maxsize
csv.field_size_limit(maxInt)

log = getLogger()


def start_token_extraction(configFilePath, paragraphFilePath, processId, workspace, message):
    try:
        start_time = get_current_time()
        config = Config_reader.read_config_file(Constants.BASE_PATH_TOOL_1 + processId+'/'+configFilePath)
        config_name = config[Constants.CONFIG_NAME]
        log.info("start_token_extraction : process started for processId == " + str(processId) + " with config name == " +
                str(config_name) + " at time == " + str(start_time))
        specific_file_header = config[Constants.SFILE_HEADER]
        sentence_end_character = config[Constants.SEC]
        regex_rules_for_token_extraction = config[Constants.REGEX_RULES]
        token_length_max = config[Constants.TOKEN_LENGTH_MAX]
        token_length_min = config[Constants.TOKEN_LENGTH_MIN]
        use_token_from_db = config[Constants.USE_TOKENS_FROM_DB]
        remove_negative_tokens = config[Constants.REMOVE_NEGATIVE_TOKEN]
        add_negative_tokens = config[Constants.ADD_NEGATIVE_TOKENS]
        insertion_order = config[Constants.TOKEN_INSERTION_ORDER]

        tokens = extract_tokens(regex_rules_for_token_extraction, Constants.BASE_PATH_TOOL_1 + processId+'/'+paragraphFilePath)
        tokens = apply_length_rules(tokens, token_length_min, token_length_max)


        negative_tokens = Utils.read_from_csv(Constants.NEGATIVE_TOKEN_FILE_PATH)
        if remove_negative_tokens :
            log.info('start_token_extraction : removing negative tokens')
            negative_tokens = negative_tokens.__add__(add_negative_tokens)
            tokens = [x for x in tokens if x not in negative_tokens]
            log.info('start_token_extraction : negative tokens removed')

        filename = write_to_csv(tokens, processId, specific_file_header, Constants.BASE_PATH_TOOL_1, workspace)
        filename_negative = write_to_csv(negative_tokens, processId, 'Negative-Token', Constants.BASE_PATH_TOOL_1, workspace)
        end_time = get_current_time()
        res = {'path': 'tokenize',
            'data': {
                'processId': processId,
                'tokenFile': filename,
                'tokenCount': len(tokens),
                'negativeTokenFile': filename_negative,
                'negativeTokenCount': len(negative_tokens)
            }}
        try:
            log.info('start_token_extraction : trying to send message to queue after token extraction')
            log.info('start_token_extraction : message == ' + str(res))
            producer = get_producer()
            producer.send(topic=Constants.EXTRACTOR_RESPONSE, value=res)
            producer.flush()
            producer.close()
            log.info("start_token_extraction : process ended for processId == " + str(processId) + " with config name == " +
                    str(config_name) + " at time == " + str(end_time))
        except Exception as e:
            log.info("start_token_extraction : ERROR  OCCURRED while sending the message to topic == "
                    + str(Constants.EXTRACTOR_RESPONSE) + " ERROR is == " + str(e))
            producer = get_producer()
            producer.send(topic=Constants.ERROR_TOPIC, value=message)
            producer.flush()
            producer.close()
    except Exception as e:
        log.info("start_token_extraction : ERROR  OCCURRED while sending the message to topic == "
                    + str(Constants.EXTRACTOR_RESPONSE) + " ERROR is == " + str(e))
        producer = get_producer()
        producer.send(topic=Constants.ERROR_TOPIC, value=message)
        producer.flush()
        producer.close()


# paragraph File will be a csv file
def extract_tokens(regexRules, paragraphFilePath):
    with open(paragraphFilePath, Constants.CSV_RT) as para:
        all_tokens = set()
        data = csv.reader(para)
        for row in data:
            text = row[0]
            tokens = apply_regex_rules(text, regexRules)
            for t in tokens:
                all_tokens.add(t)
        para.close()
    return all_tokens


def apply_regex_rules(text, regexRules):
    all_tokens = set()
    for rule in regexRules:
        text = text.lower()
        tokens = [x.group() for x in re.finditer(rule, text)]
        for t in tokens:
            all_tokens.add(t)
    return all_tokens


def apply_length_rules(tokens, min_length, max_length):
    all_tokens = set()
    for token in tokens:
        if token[-1] == "." and min_length < token.__len__() < max_length:
            token = token[0: -1]
            if not token.__contains__('.') and token.__len__() < 4:
                all_tokens.add(token)
            elif token.__contains__('.'):
                all_tokens.add(token)

    return all_tokens
