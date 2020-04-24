import multiprocessing
import threading
from multiprocessing import synchronize
from typing import Union

ThreadLock = threading.Lock
"""Type definition of a Lock variable for use between threads."""

ThreadRLock = threading.RLock
"""Type definition of an RLock (re-entrant Lock) variable for use between threads."""


class ProcessLock(synchronize.Lock):
    """Type definition of an Lock variable for use between processes.

    The issue being solved is that multiprocessing.Lock() is not a constructor, it is a method returning a synchronize.Lock. Defining this class
    enables us to treat ProcessLock as a type with a constructor, just like ThreadLock.
    """

    def __init__(self) -> None:
        super().__init__(ctx=multiprocessing.get_context())


class ProcessRLock(synchronize.RLock):
    """Type definition of an RLock (re-entrant Lock) variable for use between processes.

    The issue being solved is that multiprocessing.RLock() is not a constructor, it is a method returning a synchronize.RLock. Defining this class
    enables us to treat ProcessRLock as a type with a constructor, just like ThreadRLock.
    """

    def __init__(self) -> None:
        super().__init__(ctx=multiprocessing.get_context())


LockType = Union[ThreadLock, ProcessLock]
RLockType = Union[ThreadRLock, ProcessRLock]
