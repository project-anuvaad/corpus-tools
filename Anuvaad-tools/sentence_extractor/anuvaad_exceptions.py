class Error(Exception):
   """Base class for other exceptions"""
   pass


class WrongFilePath(Error):
   """Raised when the input value for path is incorrect"""
   pass


class KafkaErrorSending(Error):
   """Raised when the input value is too large"""
   pass