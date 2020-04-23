import sys

from tblib import pickling_support

pickling_support.install()


class TraceableException:
    """ This class stores an Exception and its traceback, allowing it to be rethrown in another process (or thread) whilst maintaining a useful stack trace """

    def __init__(self, error: BaseException):
        if error is None:
            raise ValueError("An exception must be provided")
        self._exception = error
        self._traceback = error.__traceback__ or sys.exc_info()[2]

    def __repr__(self) -> str:
        return f"TraceableException({repr(self._exception)})"

    def get_error(self) -> BaseException:
        """Returns the original exception."""
        return self._exception.with_traceback(self._traceback)
