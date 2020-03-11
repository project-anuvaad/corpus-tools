import utils.anuvaad_constants as Constants
from elastic_utils.elastic_factory import get_elastic_search_client
from utils.anuvaad_tools_logger import getLogger
from utils.timeutils import get_current_time

log = getLogger()
client = get_elastic_search_client()


def create_sentence(sentence, index):
    log.info('create_sentence : started for sentence == ' + str(sentence))
    try:
        result = client.index(index=index, id=sentence[Constants.HASH],
                              body={
                                  Constants.PROCESS_ID: [sentence[Constants.PROCESS_ID]],
                                  Constants.FILES: [sentence[Constants.FILE_NAME]],
                                  Constants.DOCUMENT_TITLE: sentence[Constants.DOCUMENT_TITLE],
                                  Constants.CREATED_DATE: sentence[Constants.CREATED_DATE],
                                  Constants.SOURCE_LANG: sentence[Constants.SOURCE_LANG],
                                  Constants.CREATED_BY: sentence[Constants.CREATED_BY],
                                  Constants.TARGET_LANGUAGE: sentence[Constants.TARGET_LANGUAGE],
                                  Constants.UPDATED_DATE: sentence[Constants.UPDATED_DATE],
                                  Constants.UPDATED_BY: sentence[Constants.UPDATED_BY],
                                  Constants.DOMAIN: sentence[Constants.DOMAIN],
                                  Constants.SOURCE_SENTENCE: sentence[Constants.SOURCE_SENTENCE],
                                  Constants.HASH: sentence[Constants.HASH],
                                  Constants.TARGET_SENTENCE: sentence[Constants.TARGET_SENTENCE],
                                  Constants.TARGET_SENTENCES: sentence[Constants.TARGET_SENTENCES]
                              })
        log.info('create_sentence  : sentence created with id = ' + result[
            '_id'] + ' at index = ' + Constants.SENTENCES_INDEX)

        return result['_id']
    except Exception as e:
        log.error('create_sentence: ERROR OCCURRED WHILE CREATING SENTENCE: for sentence : ' +
                  str(sentence) + ' at index : ' + Constants.SENTENCES_INDEX)
        raise e


def create(data, index):
    log.info('create_ : started for sentence == ' + str(data))
    try:
        result = client.index(index=index, id=data[Constants.PROCESS_ID],
                              body=data)
        log.info('create  : data created with id = ' + result[
            '_id'] + ' at index = ' + index)

        return result['_id']
    except Exception as e:
        log.error('create: ERROR OCCURRED WHILE CREATING Data for : ' +
                  str(data) + ' at index : ' + str(index))
        raise e

def get_all_by_ids(ids, index):
    start_time = get_current_time()
    log.info('get_all_by_ids : started for ids == ' + str(ids) + ', on index == ' + str(index))
    try:
        body = {'ids': ids}
        data = {}
        response = client.mget(index=index, body=body)
        docs = response[Constants.DOCS]
        for doc in docs:
            if doc[Constants.FOUND]:
                data[doc[Constants.ID]] = doc[Constants._SOURCE]
        end_time = get_current_time()
        log.info(
            'get_all_by_ids : ended for ids == ' + str(ids) + ', on index == ' + str(index) + ', total time taken '
            + '== ' + str(end_time - start_time))
        return data
    except Exception as e:
        log.error(
            'get_all_by_ids : Error occurred for ids == ' + str(ids) + ', on index == ' + str(index) + ',Error is =='
            + str(e))


def update(id_, index, body):
    start_time = get_current_time()
    log.info('update : started for id == ' + str(id_) + ', on index == ' + str(index))
    try:
        data = {'doc': body}
        response = client.update(index=index, id=id_, body=data)
        result = response[Constants.RESULT]
        end_time = get_current_time()
        log.info('update : result == ' + str(result) + ' , response from ES is == ' + str(response['_shards']))
        log.info(
            'update : ended for id == ' + str(id_) + ', on index == ' + str(index) + ', total time taken '
            + '== ' + str(end_time - start_time))
        return data
    except Exception as e:
        log.error(
            'update : Error occurred for ids == ' + str(id_) + ', on index == ' + str(index) + ',Error is =='
            + str(e))
