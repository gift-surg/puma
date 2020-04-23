import sys

# This module defines a function precision_timestamp() which returns a timestamp with the following characteristics:
# - Represents a number in seconds. The epoch (zero time) is not defined and may restart when the computer restarts.
# - Has precision of one millisecond or better
# - Is system-wide, i.e. the same across all threads and processes
# - Is unaffected by daylight saving and by the 'wall time' being changed
# - Is monotonically increasing (never decreases) unless the computer is rebooted

# On Linux, time.monotonic() satisfies all of these requirements. However on Windows:
# - time.monotonic() has only 16ms resolution on Windows
# - time.perf_counter() has high precision but is not system-wide, it starts from zero at the start of each process.
# - time.perf_counter_ns() has high precision but is not system-wide.
# - time.time() is a wall timer: it will jump when the time is changed or the clock changes.
# - pygame.time.get_ticks() has high precision but is not system-wide, it starts from zero when pygame.init is called, and this has to be called in each each process.


if sys.platform == "win32":

    from ctypes import byref, windll
    from ctypes.wintypes import LARGE_INTEGER


    # noqa: E303
    class _PrecisionTimestampWindows:  # noqa: E301
        _period: float = 0.0

        @classmethod
        def now(cls) -> float:
            """Returns a timestamp in seconds, which can safely be compared to timestamps from other threads and processes."""
            if cls._period == 0.0:
                cls._initialise_period()
            t0 = LARGE_INTEGER()
            windll.kernel32.QueryPerformanceCounter(byref(t0))
            return t0.value * cls._period

        @classmethod
        def _initialise_period(cls) -> None:
            freq = LARGE_INTEGER()
            is_counter = windll.kernel32.QueryPerformanceFrequency(byref(freq))
            if not is_counter:
                raise RuntimeError("Error instantiating presision timestamp on Windows: Failed to call QueryPerformanceFrequency")
            cls._period = 1.0 / freq.value


    # noqa: E303
    precision_timestamp = _PrecisionTimestampWindows.now
    """Returns a timestamp in seconds, which can safely be compared to timestamps from other threads and processes."""

else:
    import time

    precision_timestamp = time.monotonic
    """Returns a timestamp in seconds, which can safely be compared to timestamps from other threads and processes."""
