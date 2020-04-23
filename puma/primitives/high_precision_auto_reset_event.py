import sys

from puma.primitives import AutoResetEvent, HighPrecisionCondition

if sys.platform == "win32":
    WINDOWS_WAIT_PRECISION = 0.016
    WINDOWS_BUSY_INTERVAL = 0.001


    # noqa: E303
    class HighPrecisionAutoResetEvent(AutoResetEvent):  # noqa: E301
        # On windows, the timeout on event.wait() tends to wait for intervals that are a multiple of 15 or 16 milliseconds.
        # To get better timing performance, we have to use a more busy wait.
        def __init__(self) -> None:
            super().__init__()
            self._cond = HighPrecisionCondition()  # replace the condition with our version
else:
    HighPrecisionAutoResetEvent = AutoResetEvent
