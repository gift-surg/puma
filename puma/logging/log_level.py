import logging
from enum import Enum


class LogLevel(Enum):
    notset = logging.NOTSET
    debug = logging.DEBUG
    info = logging.INFO
    error = logging.ERROR
    warn = logging.WARN
    fatal = logging.FATAL

    def __str__(self) -> str:
        return f"{self.name.upper()}"  # uppercase is necessary for _LoggingUtilsImpl.override()
