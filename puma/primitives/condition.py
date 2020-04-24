import multiprocessing
import threading
from multiprocessing import synchronize
from typing import Union

ThreadCondition = threading.Condition
"""Type definition of a Condition variable for use between threads."""


class ProcessCondition(synchronize.Condition):
    """Type definition of a Condition variable for use between processes.

    The issue being solved is that multiprocessing.Condition() is not a constructor, it is a method returning a synchronize.Condition. Defining this class
    enables us to treat ProcessCondition as a type with a constructor, just like ThreadCondition.
    """

    def __init__(self, lock: Union[synchronize.Lock, synchronize.RLock, None] = None) -> None:
        super().__init__(lock=lock, ctx=multiprocessing.get_context())


ConditionType = Union[ThreadCondition, ProcessCondition]
