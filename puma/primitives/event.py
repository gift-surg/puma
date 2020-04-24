import multiprocessing
import threading
from multiprocessing import synchronize
from typing import Union

ThreadEvent = threading.Event
"""Type definition of an Event variable for use between threads."""


class ProcessEvent(synchronize.Event):
    """Type definition of an Event variable for use between processes.

    The issue being solved is that multiprocessing.Event() is not a constructor, it is a method returning a synchronize.Event. Defining this class
    enables us to treat ProcessEvent as a type with a constructor, just like ThreadEvent.
    """

    def __init__(self) -> None:
        super().__init__(ctx=multiprocessing.get_context())


EventType = Union[ThreadEvent, ProcessEvent]
