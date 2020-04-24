import multiprocessing
from typing import Union, cast, no_type_check

from puma.primitives import ProcessRLock, ThreadRLock


class ProcessSafeInt:
    """An integer value shared across processes using shared-memory. Access is process-safe, but updates (e.g. increment) need to be locked using get_lock().

    The API is based on multiprocessing.Value.
    """

    def __init__(self, initial_value: int):
        self._val = multiprocessing.Value('i', initial_value)

    @property
    def value(self) -> int:
        return cast(int, self._val.value)

    @value.setter
    def value(self, val: int) -> None:
        self._val.value = val

    # For some reason get_lock is not defined in the type hints file for multiprocessing.Value, although it is present and included in the docs (in the example code)
    # noinspection PyTypeChecker
    @no_type_check
    def get_lock(self) -> ProcessRLock:
        return self._val.get_lock()


class ThreadSafeInt:
    """An integer value shared across threads using shared-memory. Access is thread-safe, but updates (e.g. increment) need to be locked using get_lock().

    The API is based on multiprocessing.Value.
    """

    def __init__(self, initial_value: int):
        self._val = initial_value
        self._lock = ThreadRLock()

    @property
    def value(self) -> int:
        with self._lock:
            return self._val

    @value.setter
    def value(self, val: int) -> None:
        with self._lock:
            self._val = val

    def get_lock(self) -> ThreadRLock:
        return self._lock


class ProcessSafeBool:
    """A boolean value shared across processes using shared-memory. Access is process-safe, but updates (e.g. invert) need to be locked using get_lock().

    The API is based on multiprocessing.Value.
    """

    def __init__(self, initial_value: bool):
        self._val = multiprocessing.Value('i', int(initial_value))

    @property
    def value(self) -> bool:
        return bool(self._val.value)

    @value.setter
    def value(self, val: bool) -> None:
        self._val.value = int(val)

    # For some reason get_lock is not defined in the type hints file for multiprocessing.Value, although it is present and included in the docs (in the example code)
    # noinspection PyTypeChecker
    @no_type_check
    def get_lock(self) -> ProcessRLock:
        return self._val.get_lock()


class ThreadSafeBool:
    """A boolean value shared across thread using shared-memory. Access is process-safe, but updates (e.g. invert) need to be locked using get_lock().

    The API is based on multiprocessing.Value.
    """

    def __init__(self, initial_value: bool):
        self._val = initial_value
        self._lock = ThreadRLock()

    @property
    def value(self) -> bool:
        with self._lock:
            return self._val

    @value.setter
    def value(self, val: bool) -> None:
        with self._lock:
            self._val = val

    def get_lock(self) -> ThreadRLock:
        return self._lock


SafeIntType = Union[ThreadSafeInt, ProcessSafeInt]
SafeBoolType = Union[ThreadSafeBool, ProcessSafeBool]
