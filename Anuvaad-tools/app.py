from flask import Flask

from sentence_extractor.tokens_and_sentences_ext_runner import tokens_and_sentences_ext_thread
from machine_translator.machine_translator_runner import machine_translation_thread
from search_replace_and_composition.search_replace_runner import search_replace_and_composition_thread
from utils.anuvaad_tools_logger import getLogger
from api.tool_server_check_api import health_check_api
from api.machine_translation_api import mt_api
from mongo_utils.mongo_connect import connect_mongo
import threading

app = Flask(__name__)
app.register_blueprint(health_check_api)
app.register_blueprint(mt_api)

log = getLogger()
connect_mongo()

try:
    log.info('Starting Threads ')
    t1 = threading.Thread(target=tokens_and_sentences_ext_thread, name='tokens_and_sentences_extractor')
    t1.start()
    log.info('tokens_and_sentences_extractor started')
    # t2 = threading.Thread(target=machine_translation_thread, name='machine_translation')
    # t2.start()
    # log.info('machine_translation started')
    t3 = threading.Thread(target=search_replace_and_composition_thread, name='search_replace_and_composition')
    t3.start()
    log.info('search_replace_and_composition started')

    log.info('all_thread started ')

except Exception as e:
    log.info('ERROR WHILE RUNNING CUSTOM THREADS '+str(e))


if __name__ == '__main__':
    app.run()
