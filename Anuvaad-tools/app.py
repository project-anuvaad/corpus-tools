from flask import Flask

from sentence_extractor.token_ext_runner import extract_tokens_thread
from sentence_extractor.sentence_ext_runner import extract_sentences_thread
from utils.anuvaad_tools_logger import getLogger
from api.tool_server_check_api import health_check_api
import threading

app = Flask(__name__)
app.register_blueprint(health_check_api)

log = getLogger()

try:
    log.info('Starting Threads ')
    t1 = threading.Thread(target=extract_tokens_thread, name='keep_on_running')
    t1.start()
    log.info('extract_token_thread started')
    t2 = threading.Thread(target=extract_sentences_thread, name='keep_on_running')
    t2.start()
    log.info('extract_sentences_thread started ')

except Exception as e:
    log.info('ERROR WHILE RUNNING CUSTOM THREADS '+str(e))


if __name__ == '__main__':
    app.run()
