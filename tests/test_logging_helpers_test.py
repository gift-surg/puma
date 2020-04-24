import logging
from unittest import TestCase

from tests.test_logging_helpers import assert_no_warnings_or_errors_logged

logger = logging.getLogger(__name__)


class TestLoggingHelpersTest(TestCase):
    @assert_no_warnings_or_errors_logged
    def test_assert_no_warnings_or_errors_logged_decorator_when_debug(self) -> None:
        logger.debug("No problem")

    @assert_no_warnings_or_errors_logged
    def test_assert_no_warnings_or_errors_logged_decorator_when_debug_root(self) -> None:
        logging.debug("No problem")

    def test_assert_no_warnings_or_errors_logged_decorator_when_warning(self) -> None:
        with self.assertRaisesRegex(AssertionError, "Unexpectedly raised warnings or errors"):
            self._should_assert_because_of_warning()

    def test_assert_no_warnings_or_errors_logged_decorator_when_warning_root(self) -> None:
        with self.assertRaisesRegex(AssertionError, "Unexpectedly raised warnings or errors"):
            self._should_assert_because_of_warning_root()

    def test_assert_no_warnings_or_errors_logged_decorator_when_error(self) -> None:
        with self.assertRaisesRegex(AssertionError, "Unexpectedly raised warnings or errors"):
            self._should_assert_because_of_error()

    def test_assert_no_warnings_or_errors_logged_decorator_when_error_root(self) -> None:
        with self.assertRaisesRegex(AssertionError, "Unexpectedly raised warnings or errors"):
            self._should_assert_because_of_error_root()

    def test_assert_no_warnings_or_errors_logged_function_when_debug(self) -> None:
        with assert_no_warnings_or_errors_logged(self):
            logger.debug("No problem")

    def test_assert_no_warnings_or_errors_logged_function_when_debug_root(self) -> None:
        with assert_no_warnings_or_errors_logged(self):
            logging.debug("No problem")

    def test_assert_no_warnings_or_errors_logged_function_when_warning(self) -> None:
        with self.assertRaisesRegex(AssertionError, "Unexpectedly raised warnings or errors"):
            with assert_no_warnings_or_errors_logged(self):
                logger.warning("Problem")

    def test_assert_no_warnings_or_errors_logged_function_when_warning_root(self) -> None:
        with self.assertRaisesRegex(AssertionError, "Unexpectedly raised warnings or errors"):
            with assert_no_warnings_or_errors_logged(self):
                logging.warning("Problem")

    def test_assert_no_warnings_or_errors_logged_function_when_error(self) -> None:
        with self.assertRaisesRegex(AssertionError, "Unexpectedly raised warnings or errors"):
            with assert_no_warnings_or_errors_logged(self):
                logger.error("Big Problem")

    def test_assert_no_warnings_or_errors_logged_function_when_error_root(self) -> None:
        with self.assertRaisesRegex(AssertionError, "Unexpectedly raised warnings or errors"):
            with assert_no_warnings_or_errors_logged(self):
                logging.error("Big Problem")

    @assert_no_warnings_or_errors_logged
    def _should_assert_because_of_warning(self) -> None:
        logger.warning("Problem")

    @assert_no_warnings_or_errors_logged
    def _should_assert_because_of_warning_root(self) -> None:
        logging.warning("Problem")

    @assert_no_warnings_or_errors_logged
    def _should_assert_because_of_error(self) -> None:
        logger.error("Big Problem")

    @assert_no_warnings_or_errors_logged
    def _should_assert_because_of_error_root(self) -> None:
        logging.error("Big Problem")
