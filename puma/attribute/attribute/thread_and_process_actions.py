from enum import Enum, auto


class ThreadAction(Enum):
    SHARED = auto()
    COPIED = auto()
    NOT_ALLOWED = auto()


class ProcessAction(Enum):
    COPIED = auto()
    SET_TO_NONE = auto()
    NOT_ALLOWED = auto()
