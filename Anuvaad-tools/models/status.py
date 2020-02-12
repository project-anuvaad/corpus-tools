import enum


class Status(enum.Enum):
    SUCCESS = {'ok': True, 'http': {'status': 200},
               'why': "request successful"}
    FAILURE = {'ok': False, 'http': {'status': 500},
               'why': 'request failed'}
    FAILURE_FILE_NOT_FOUND = {'ok': False, 'http': {'status': 500},
               'why': 'file not found'}
    FAILURE_MANDATORY_PARAM_MISSING = {'ok': False, 'http': {'status': 500},
               'why': 'mandatory parameter missing'}

