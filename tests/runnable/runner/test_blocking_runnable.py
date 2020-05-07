import logging
from typing import Optional

from puma.attribute import copied, unmanaged
from puma.helpers.testing.mixin import NotATestCase
from puma.primitives import EventType
from puma.runnable import Runnable
from puma.runnable.runner.runner import DEFAULT_FINAL_JOIN_TIMEOUT
from tests.runnable.runner.test_execution_mode import TestExecutionMode

logger = logging.getLogger(__name__)


class TestBlockingRunnable(Runnable, NotATestCase):
    """An implementation of Runnable, for testing the thread and process Runners. Execute() blocks until told to stop.

    It does not use an owner-to-runner command buffer, instead it uses an event to stop the runner.
    There is also a set of Events that record the status of the thread/process, rather than relying on the status buffer.
    """
    _stop_event: EventType = unmanaged("_stop_event")
    _ran_event: Optional[EventType] = unmanaged("_ran_event")
    _ended_event: Optional[EventType] = unmanaged("_ended_event")
    _execution_mode: TestExecutionMode = copied("_execution_mode")

    def __init__(self, name: str, *,
                 stop_event: EventType,
                 ran_event: Optional[EventType] = None,
                 ended_event: Optional[EventType] = None,
                 execution_mode: TestExecutionMode = TestExecutionMode.WaitUntilStopped) -> None:
        super().__init__(name, [])
        self._stop_event = stop_event
        self._ran_event = ran_event
        self._ended_event = ended_event
        self._execution_mode = execution_mode

    def _execute(self) -> None:
        if self._ran_event:
            self._ran_event.set()
        try:
            if self._execution_mode == TestExecutionMode.EndQuicklyWithoutError:
                pass  # nothing to do
            elif self._execution_mode == TestExecutionMode.EndQuicklyWithError:
                raise RuntimeError("Test Error")
            elif self._execution_mode == TestExecutionMode.WaitUntilStopped:
                if not self._stop_event.wait(DEFAULT_FINAL_JOIN_TIMEOUT + 10.0):
                    raise RuntimeError("Test error: Should never get here")
            elif self._execution_mode == TestExecutionMode.IgnoreStopCommand:
                self._stop_event.wait(DEFAULT_FINAL_JOIN_TIMEOUT + 10.0)
        finally:
            if self._ended_event:
                self._ended_event.set()

    def stop(self) -> None:
        if self._execution_mode != TestExecutionMode.IgnoreStopCommand:
            self._stop_event.set()
