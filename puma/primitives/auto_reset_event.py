import threading
from typing import NoReturn, Union, no_type_check


class AutoResetEvent(threading.Event):
    """Like threading.Event, except that wait() resets the event automatically."""

    @no_type_check  # I don't know why MyPy refuses to believe that threading.Event has properties self._cond and self._flag.
    def wait(self, timeout: Union[int, float, None] = None) -> bool:
        # This is a copy of the implementation in threading.Event, but with the addition of the line that clears the flag.
        with self._cond:
            signaled: bool = self._flag
            if not signaled:
                signaled = self._cond.wait(timeout)
            # either the flag was already set, or it has just been set by set() while we were waiting, or it is not set and we timed out;
            # in every case, we should leave with the flag cleared.
            self._flag = False
            return signaled

    def __getstate__(self) -> NoReturn:
        # Raise an error if this event is shared between processes.
        raise RuntimeError(f"{self.__class__.__name__} cannot be shared across processes")
