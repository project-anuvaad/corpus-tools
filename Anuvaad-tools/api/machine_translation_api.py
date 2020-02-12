from flask import Blueprint, request
import utils.anuvaad_constants as Constants
import logging
from utils.timeutils import get_current_time
from models.response import CustomResponse
from models.status import Status
from machine_translator.machine_translator import check_file_validity, get_index, read_csv, get_hash, \
    check_elastic_for_new_sentences

mt_api = Blueprint('mt_api', __name__)
log = logging.getLogger()


@mt_api.route('/calculate-stats', methods=['POST'])
def calculate_stats():
    start_time = get_current_time()
    body = request.get_json()
    if not validate_calculate_stats(body):
        res = CustomResponse(Status.FAILURE_MANDATORY_PARAM_MISSING.value, body)
        log.info('calculate_stats : Mandatory parameters missing ')
        return res.getres()
    processId = body[Constants.SESSION_ID]
    log.info('calculate_stats : for processId' + str(processId) + ' started at == ' + str(start_time))
    sourceFiles = body[Constants.FILES]
    targetLanguage = body[Constants.TARGET_LANGUAGE]
    try:
        validation = check_file_validity(processId, sourceFiles)
        if validation[Constants.STATUS]:
            unique_ = set()
            index = get_index(targetLanguage)
            for file in sourceFiles:
                data = read_csv(file, processId)
                for text in data:
                    hash_ = get_hash(text)
                    if not unique_.__contains__(hash_):
                        unique_.add(hash_)
            already_present = check_elastic_for_new_sentences(unique_, index)
            total = len(unique_)
            to_fetch = total - already_present
            message = {

                Constants.PROCESS_ID: processId,
                Constants.TOTAL: total,
                Constants.TO_FETCH: to_fetch,
                Constants.ALREADY_PRESENT: already_present
            }
            res = CustomResponse(Status.SUCCESS.value, message)
            end_time = get_current_time()
            log.info('calculate_stats : for processId' + str(processId) + ' ended at == ' + str(end_time) +
                     ' , total time elapsed == ' + str(end_time-start_time))
            return res.getres()
        else:
            res = CustomResponse(Status.FAILURE_FILE_NOT_FOUND.value, None)
            end_time = get_current_time()
            log.info('calculate_stats : for processId' + str(processId) + ' ended at == ' + str(end_time) +
                     ' , total time elapsed == ' + str(end_time - start_time))
            return res.getres()
    except Exception as e:
        log.info('calculate_stats : Error Occurred for processId' + str(processId) + ', error is  == ' + str(e))
        res = CustomResponse(Status.FAILURE.value, e)
        return res.getres()


def validate_calculate_stats(body):
    try:
        process_id = body[Constants.SESSION_ID]
        target_language = body[Constants.TARGET_LANGUAGE]
        files = body[Constants.FILES]
        return True
    except Exception as e:
        return False