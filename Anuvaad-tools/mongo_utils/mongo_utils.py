from mongo_utils.sentence import Sentence
from mongo_utils.corpus import Corpus
from utils.anuvaad_tools_logger import getLogger
from utils.timeutils import get_current_time
import utils.anuvaad_constants as Constants

log = getLogger()


def create_sentence_entry_for_translator(processid, sentences):
    log.info('create_sentence_entry_for_translator : started for processid == ' + str(processid))
    status = Constants.PROCESSING
    for sen in sentences:
        source = sen[Constants.SOURCE]
        target = sen[Constants.TARGET]
        hash_ = sen[Constants.HASH]
        data = Sentence(source=source, target=target, basename=processid, corpusid=processid, status=status, hash=hash_,
                        completed=False)
        data.save()
    log.info('create_sentence_entry_for_translator : ended for processid == ' + str(processid))
