from puma.context import ContextManager, ensure_used_within_context_manager, must_be_context_managed
from puma.precision_timestamp.precision_timestamp import precision_timestamp

START_TIME_NOT_SET = -1.


@must_be_context_managed
class Timer(ContextManager["Timer"]):
    """A ContextManager for timing how long a section of code takes to complete"""

    def __init__(self) -> None:
        self._start_time = START_TIME_NOT_SET

    def __enter__(self) -> "Timer":
        self.__set_start_time(self.__now_source())
        return self

    def __now_source(self) -> float:
        return precision_timestamp()

    def __set_start_time(self, timestamp: float) -> None:
        self._start_time = timestamp

    @ensure_used_within_context_manager
    def sub_timer(self) -> "Timer":
        return Timer()

    @ensure_used_within_context_manager
    def get_elapsed_time(self) -> float:
        return self.__now_source() - self._start_time
