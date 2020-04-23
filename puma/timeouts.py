from time import monotonic
from typing import Callable, Optional, TypeVar, Union

TIMEOUT_NO_WAIT = -123579.2  # just some unlikely value
"""Call without blocking, failing if the action cannot be completed immediately."""

TIMEOUT_INFINITE = -793147.1  # just some unlikely value
"""Wait forever"""

WAIT_MAX = 2147483.0
"""This is the maximum timeout that Condition.wait() accepts. This is 2^31/1000 seconds i.e. 2^31 milliseconds, and is half of threading.TIMEOUT_MAX."""


class Timeouts:
    """Utility class dealing with timeout values."""

    @staticmethod
    def is_blocking(timeout: float) -> bool:
        """Given a timeout value, checks that it is legal then returns True unless the timeout equals TIMEOUT_NO_WAIT."""
        Timeouts.validate(timeout)
        return timeout != TIMEOUT_NO_WAIT

    @staticmethod
    def timeout_for_queue(timeout: float) -> Optional[float]:
        """Given a timeout value, checks that it is legal then returns the value to pass to the timeout parameter of Queue.put() or Condition.wait()"""
        Timeouts.validate(timeout)
        if timeout == TIMEOUT_NO_WAIT:
            return 0.0  # For a Queue this will be ignored since the 'blocking' parameter should be False
        if timeout == TIMEOUT_INFINITE:
            return None
        if timeout >= WAIT_MAX:
            return None
        return timeout

    @staticmethod
    def end_time(now_time: float, timeout: float) -> float:
        """Given the current time (as returned by monotonic() or perf_counter()) and a timeout value, checks that it is legal then returns the time when the timeout should end."""
        Timeouts.validate(timeout)
        if timeout == TIMEOUT_NO_WAIT:
            return now_time
        if timeout == TIMEOUT_INFINITE:
            return now_time + WAIT_MAX
        return now_time + timeout

    @staticmethod
    def describe(timeout: float) -> str:
        """Human-readable description of a timeout value"""
        if timeout == TIMEOUT_NO_WAIT:
            return "TIMEOUT_NO_WAIT"
        if timeout == TIMEOUT_INFINITE:
            return "TIMEOUT_INFINITE"
        if timeout >= WAIT_MAX:
            return "TIMEOUT_INFINITE"
        return str(timeout)

    @staticmethod
    def describe_optional(timeout: Optional[float]) -> str:
        """Human-readable description of a timeout value where "optional float" is potentially used instead of the special values.

        This method of representing a timeout is potentially more error-prone than using the special values, since the value zero has different meanings to different
        API calls.
        """
        if timeout is None:
            return "Forever"
        if timeout == 0.0:
            return "Zero time"
        return Timeouts.describe(timeout)

    @staticmethod
    def validate(timeout: float) -> None:
        if timeout is None:
            raise ValueError("Timeout cannot be None. Use TIMEOUT_INFINITE instead.")
        if not isinstance(timeout, (float, int)):
            raise ValueError("Illegal parameter for timeout")
        if timeout == 0.0:
            raise ValueError("Timeout value must not be zero. Use TIMEOUT_NO_WAIT or TIMEOUT_INFINITE as appropriate.")
        if (timeout < 0.0) and (timeout != TIMEOUT_NO_WAIT) and (timeout != TIMEOUT_INFINITE):
            raise ValueError("Timeout value must be greater than zero, or one of the special values TIMEOUT_NO_WAIT or TIMEOUT_INFINITE.")

    @staticmethod
    def validate_optional(timeout: Optional[float]) -> None:
        """Check that an optional timeout value is legal"""
        if timeout is not None:
            if not isinstance(timeout, (float, int)):
                raise ValueError("Illegal parameter for timeout")
            if timeout < 0.0:
                raise ValueError("Timeout must not be negative")

    T = TypeVar("T")

    class ReenterTimeoutLoopType:
        pass

    REENTER_TIMEOUT_LOOP = ReenterTimeoutLoopType()

    @staticmethod
    def timeout_loop(timeout: float, callback: Callable[[float], Union[T, ReenterTimeoutLoopType]], now_source: Callable[[], float] = monotonic) -> T:
        now = now_source()
        loop_end_time = Timeouts.end_time(now, timeout)
        while now < loop_end_time:
            new_timeout = loop_end_time - now
            result = callback(new_timeout)
            if isinstance(result, Timeouts.ReenterTimeoutLoopType):
                now = now_source()
                continue
            else:
                return result
        raise TimeoutError("Timed out")
