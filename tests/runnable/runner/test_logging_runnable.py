import logging
import time

from puma.attribute import child_only, child_scope_value
from puma.runnable import Runnable
from puma.runnable.runner import ProcessRunner
from tests.logging.capture_logs import CaptureLogs
from tests.mixin import NotATestCase
from tests.runnable.test_support.parameterisation import ProcessRunnerTestEnvironment


class TestLoggingRunnable(Runnable, NotATestCase):
    """An implementation of Runnable, for testing the logging in thread and process Runners."""
    _launch_child_runners_depth: int = child_only("_launch_child_runners_depth")
    _depth: int = child_only("_depth")
    _capture_and_relog: bool = child_only("_capture_and_relog")
    _delay: float = child_only("_delay")

    def __init__(self, name: str, *, launch_child_runners_depth: int = 0, depth: int = 0, capture_and_relog: bool = False, delay: float = 0.0) -> None:
        super().__init__(name, [])
        self._launch_child_runners_depth = child_scope_value(launch_child_runners_depth)
        self._depth = child_scope_value(depth)
        self._capture_and_relog = child_scope_value(capture_and_relog)
        self._delay = child_scope_value(delay)

    def _execute(self) -> None:
        logger = logging.getLogger(__name__)

        time.sleep(self._delay)

        if self._capture_and_relog:
            with CaptureLogs() as logging_context:
                self._do_logging(logger)
                records = logging_context.pop_captured_records()
                for line in records.get_lines(prefix='!!', timestamp=False, level=False, line_separators=False):
                    logger.debug(line)
        else:
            self._do_logging(logger)

        if self._depth > 0:
            logger.debug(f"Debug message at depth {self._depth}")

        ancestor_depth_needed = self._launch_child_runners_depth - self._depth
        if ancestor_depth_needed > 0:
            child_runnable = TestLoggingRunnable(self.get_name() + "+", launch_child_runners_depth=self._launch_child_runners_depth, depth=self._depth + 1)
            with ProcessRunner(child_runnable) as child:
                child.start_blocking()
                child.join(ProcessRunnerTestEnvironment().activity_timeout() * ancestor_depth_needed)
                child.check_for_exceptions()

    @staticmethod
    def _do_logging(logger: logging.Logger) -> None:
        logger.debug("Debug message")
        logger.warning("Warning message")
        logger.error("Error message")
