from enum import Enum, auto


class AccessibleScope(Enum):
    parent = auto()
    child = auto()
    shared = auto()
