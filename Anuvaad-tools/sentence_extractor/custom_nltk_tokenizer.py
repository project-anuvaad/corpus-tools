import nltk
from utils.anuvaad_tools_logger import getLogger

log = getLogger()


def get_tokenizer_english_pickle():
    log.info('get_tokenizer_english_pickle : loading tokenizers/punkt/english.pickle ')
    tokenizer = nltk.data.load('tokenizers/punkt/english.pickle')
    log.info('get_tokenizer_english_pickle : loading completed for tokenizers/punkt/english.pickle ')
    return tokenizer


def update_english_pickle_with_tokens(tokenizer, tokens, exclusive_tokens):
    log.info('update_english_pickle_with_tokens : updating tokenizer')
    tokens.sort(key=len, reverse=True)
    for token in tokens:
        tokenizer._params.abbrev_types.add(token)
    for token in exclusive_tokens:
        tokenizer._params.abbrev_types.add(token)
    log.info('update_english_pickle_with_tokens : tokenizer updated')
    return tokenizer

