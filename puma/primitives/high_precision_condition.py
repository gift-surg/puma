import sys
import time
from typing import Callable, TypeVar, Union

from puma.primitives import ThreadCondition

# This module implements HighPrecisionCondition, which is a multi-threading Condition variable that provides at least one millisecond precision in its wait() method.
# The condition may be slightly more CPU-intensive than the standard condition, which may be noticeable if short intervals are waited on repeatedly.


if sys.platform == "win32":
    # On Windows, the wait() method has a precision of about 15ms. For example, wait(0.01) will either return immediately or wait for 0.015 seconds.
    # To achieve the required precision, we use wait() for the longest interval we dare (because this uses no CPU resources), and then we go into a fast loop,
    # sleeping for small chunks of time (fortunately, sleep() has high precision).

    WINDOWS_WAIT_PRECISION = 0.016
    WINDOWS_BUSY_INTERVAL = 0.001

    _PT = TypeVar('_PT')


    # noqa: E303
    class HighPrecisionCondition(ThreadCondition):  # noqa: E301
        """A multi-threading Condition variable that provides at least one millisecond precision in its wait() method"""

        def wait(self, timeout: Union[int, float, None] = None) -> bool:
            entry_time = time.perf_counter()

            if not timeout:  # either zero or None: in both cases use the default behaviour
                return super().wait(timeout)

            end_time = entry_time + timeout

            # wait for as long as we dare using the default mechanism
            long_wait = timeout - WINDOWS_WAIT_PRECISION - (2 * WINDOWS_BUSY_INTERVAL)
            if long_wait > 0.0 and super().wait(long_wait):
                return True

            # now go into a busy loop for the remaining few milliseconds
            while time.perf_counter() <= end_time:
                if super().wait(0.0):
                    return True
                time.sleep(WINDOWS_BUSY_INTERVAL)
            return super().wait(0.0)

        def wait_for(self, predicate: Callable[[], _PT], timeout: Union[int, float, None] = None) -> _PT:
            # Implementation copied from thread.Condition, with _time() changed to time.perf_counter()
            endtime = None
            waittime = timeout
            result = predicate()
            while not result:
                if waittime is not None:
                    if endtime is None:
                        endtime = time.perf_counter() + waittime
                    else:
                        waittime = endtime - time.perf_counter()
                        if waittime <= 0:
                            break
                self.wait(waittime)
                result = predicate()
            return result
else:
    # On Linux, ThreadCondition satisfies the requirements and can be used directly.
    HighPrecisionCondition = ThreadCondition
    """A multi-threading Condition variable that provides at least one millisecond precision in its wait() method"""
