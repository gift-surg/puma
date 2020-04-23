import logging
from unittest import TestCase

from puma.logging import LogLevel, Logging
from tests.logging.capture_logs import CaptureLogs


# A partner to LoggingUtilsTest, but LoggingUtils.init_logging is never called, since calling it and then calling reset_logging doesn't
# always get you back to where you started.
# Also, a logger used in the main application gets called "__main__", but we don't normally come across this in unit tests, so cover this explicitly.


class LoggingNotInitialisedTest(TestCase):

    def setUp(self) -> None:
        # We want the logging system at its default state, as it would be in an application, so undo the test framework's changes
        Logging.reset_logging()

        # Resetting the logging system would invalidate any module level loggers, so instead get the loggers now
        self._logger = logging.getLogger(__name__)
        self._main_logger = logging.getLogger("__main__")

    def test_default_logging(self) -> None:
        # The assumption is that default logging is at warning level - check this assumption
        with CaptureLogs() as log_context:
            self._do_logging()
            lines = log_context.pop_captured_records().get_lines(timestamp=False, level=False)
            self.assertEqual(["Test case: Warning message", "Test case: Error message",
                              "Main: Warning message", "Main: Error message",
                              "Root: Warning message", "Root: Error message"], lines)

    def test_override_global_level_up(self) -> None:
        Logging.override_global_level(LogLevel.error)
        with CaptureLogs() as log_context:
            self._do_logging()
            lines = log_context.pop_captured_records().get_lines(timestamp=False, level=False)
            self.assertEqual(["Test case: Error message",
                              "Main: Error message",
                              "Root: Error message"], lines)

    def test_override_global_level_down(self) -> None:
        Logging.override_global_level(LogLevel.debug)
        with CaptureLogs() as log_context:
            self._do_logging()
            lines = log_context.pop_captured_records().get_lines(timestamp=False, level=False)
            self.assertEqual(["Test case: Debug message", "Test case: Warning message", "Test case: Error message",
                              "Main: Debug message", "Main: Warning message", "Main: Error message",
                              "Root: Debug message", "Root: Warning message", "Root: Error message"], lines)

    def test_override_sections(self) -> None:
        Logging.override_sections({'root': LogLevel.debug, 'a': LogLevel.debug, '__main__': LogLevel.error, 'tests': LogLevel.error, 'a.b.c': LogLevel.warn})
        logger_ab = logging.getLogger('a.b')
        logger_abc = logging.getLogger('a.b.c')
        logger_abcd = logging.getLogger('a.b.c.d')
        with CaptureLogs() as log_context:
            self._do_logging_with(logger_ab, "a.b")
            self._do_logging_with(logger_abc, "a.b.c")
            self._do_logging_with(logger_abcd, "a.b.c.d")
            self._do_logging()
            lines = log_context.pop_captured_records().get_lines(timestamp=False, level=False)
            self.assertEqual(["a.b: Debug message", "a.b: Warning message", "a.b: Error message",
                              "a.b.c: Warning message", "a.b.c: Error message",
                              "a.b.c.d: Warning message", "a.b.c.d: Error message",
                              "Test case: Error message",
                              "Main: Error message",
                              "Root: Debug message", "Root: Warning message", "Root: Error message"], lines)

    def test_override_root(self) -> None:
        Logging.override_root(LogLevel.error)  # Logging not initialised, so all logging delegates to root
        with CaptureLogs() as log_context:
            self._do_logging()
            lines = log_context.pop_captured_records().get_lines(timestamp=False, level=False)
            self.assertEqual(["Test case: Error message",
                              "Main: Error message",
                              "Root: Error message"], lines)

    def _do_logging(self) -> None:
        self._do_logging_with(self._logger, "Test case")
        self._do_logging_with(self._main_logger, "Main")
        logging.debug("Root: Debug message")
        logging.warning("Root: Warning message")
        logging.error("Root: Error message")

    @classmethod
    def _do_logging_with(cls, logger: logging.Logger, prefix: str) -> None:
        logger.debug(f"{prefix}: Debug message")
        logger.warning(f"{prefix}: Warning message")
        logger.error(f"{prefix}: Error message")
