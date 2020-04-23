from puma.attribute import copied
from puma.runnable import Runnable
from tests.mixin import NotATestCase


class TestInlineRunnable(Runnable, NotATestCase):
    """A non-blocking implementation of Runnable, for testing the Runner. Execute() finishes instantaneously.

    It does not use an owner-to-runner command buffer, instead it uses a flag to indicate if the runner was told to stop.
    """
    _simulate_stop_not_working: bool = copied("_simulate_stop_not_working")
    _raise_error: bool = copied("_raise_error")
    executed: bool = copied("executed")
    stopped: bool = copied("stopped")

    def __init__(self, name: str, *, simulate_stop_not_working: bool = False, raise_error: bool = False):
        super().__init__(name, [])
        self._simulate_stop_not_working = simulate_stop_not_working
        self._raise_error = raise_error
        self.executed = False
        self.stopped = False

    def _execute(self) -> None:
        self.executed = True
        if self._raise_error:
            raise RuntimeError("Test Error")

    def stop(self) -> None:
        if not self._simulate_stop_not_working:
            self.stopped = True
