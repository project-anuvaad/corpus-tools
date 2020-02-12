from flask import jsonify


class CustomResponse:
    def __init__(self, statuscode, data, count=0):
        self.statuscode = statuscode
        self.statuscode['data'] = data

    def getres(self):
        return jsonify(self.statuscode)
