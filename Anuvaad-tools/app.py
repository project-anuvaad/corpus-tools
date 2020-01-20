from flask import Flask

from sentence_extractor.token_ext_runner import extract_tokens_thread
from sentence_extractor.sentence_ext_runner import extract_sentences_thread
from machine_translator.machine_translator_runner import translation_fetcher_and_writer_thread
from machine_translator.machine_translator_runner import machine_translation_thread
from search_replace.search_replace_runner import search_replace_thread
from utils.anuvaad_tools_logger import getLogger
from api.tool_server_check_api import health_check_api
from mongo_utils.mongo_connect import connect_mongo
import threading

app = Flask(__name__)
app.register_blueprint(health_check_api)

log = getLogger()
connect_mongo()

try:
    log.info('Starting Threads ')
    t1 = threading.Thread(target=extract_tokens_thread, name='token_extractor')
    t1.start()
    log.info('extract_token_thread started')
    t2 = threading.Thread(target=extract_sentences_thread, name='sentence_extractor')
    t2.start()
    t4 = threading.Thread(target=translation_fetcher_and_writer_thread, name='machine_translation_2')
    t4.start()
    t3 = threading.Thread(target=machine_translation_thread, name='machine_translation_1')
    t3.start()
    t5 = threading.Thread(target=search_replace_thread, name='search and replace')
    t5.start()


    log.info('all_thread started ')

except Exception as e:
    log.info('ERROR WHILE RUNNING CUSTOM THREADS '+str(e))


if __name__ == '__main__':
    app.run()
