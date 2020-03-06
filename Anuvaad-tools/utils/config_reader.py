import yaml
from utils.anuvaad_tools_logger import getLogger

log = getLogger()


def read_config_file(filepath='resources/tool_1_config.yaml'):
    with open(filepath) as file:
        # The FullLoader parameter handles the conversion from YAML
        # scalar values to Python the dictionary format
        config = yaml.load(file, Loader=yaml.FullLoader)
        log.info('read_config_file : filepath is == ' + str(filepath))
        log.info('read_config_file : config is == ' + str(config))
        file.close()
        return config

