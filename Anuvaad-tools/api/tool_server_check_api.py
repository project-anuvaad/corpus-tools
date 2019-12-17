from flask import Blueprint
from utils.anuvaad_tools_logger import getLogger
from sentence_extractor import token_extractor as SEN
health_check_api = Blueprint('health_check_api', __name__)
log = getLogger()

c = '/home/mayank/PycharmProjects/Anuvaad-tools/resources/tool_1_config.yaml'
p = '/home/mayank/PycharmProjects/Anuvaad-tools/resources/raw_para.csv'

@health_check_api.route('/tools-test')
def tool_health():
    log.info(" Tool Test Api called ")
    SEN.start_token_extraction(c, p, '1')
    return ' SERVER IS RUNNING '
