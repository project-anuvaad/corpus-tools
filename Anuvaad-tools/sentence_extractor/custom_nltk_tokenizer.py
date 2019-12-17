import nltk
from nltk.tokenize.punkt import PunktSentenceTokenizer, PunktLanguageVars
from utils.anuvaad_tools_logger import getLogger
from utils.timeutils import get_current_time

log = getLogger()


def get_tokenizer_english_pickle():
    log.info('get_tokenizer_english_pickle : loading tokenizers/punkt/english.pickle ')
    tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
    log.info('get_tokenizer_english_pickle : loading completed for tokenizers/punkt/english.pickle ')
    return tokenizer


def update_english_pickle_with_tokens(tokenizer, tokens):
    log.info('update_english_pickle_with_tokens : updating tokenizer')
    for token in tokens:
        tokenizer._params.abbrev_types.add(token)
    log.info('update_english_pickle_with_tokens : tokenizer updated')
    return tokenizer

#
# class BulletPointLangVars(PunktLanguageVars):
#     sent_end_chars = ('.', '?', '!', ';')



# Token Extraction


