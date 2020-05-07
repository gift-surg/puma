import logging
import re
from io import StringIO
from time import sleep
from typing import Callable
from unittest import TestCase, mock

from puma.helpers.testing.logging.capture_logs import CaptureLogs
from puma.helpers.timer import time
from puma.logging import LogLevel, Logging
from tests.github_issue_11_quickfix import skip_on_windows_until_github_issue_11_is_resolved

logger = logging.getLogger(__name__)


class TimerDecoratorTest(TestCase):

    @time()
    def decorated_method(self) -> None:
        sleep(0.25)

    @time(output_method=logger.warning)
    def decorated_method_with_custom_output_method(self) -> None:
        sleep(0.25)

    @time(description="String Description")
    def decorated_method_with_string_description(self) -> None:
        sleep(0.25)

    @time(description=lambda m: m.__name__.upper())
    def decorated_method_with_callable_description(self) -> None:
        sleep(0.25)

    @time()
    def decorated_method_takes_params(self, arg_1: str, arg_2: int) -> None:
        sleep(0.25)

    @time()
    def decorated_method_with_docstring(self) -> None:
        """This is the docstring"""
        sleep(0.25)

    def _assert_method_timing_printed_correctly(self, method: Callable[[], None], expected_output: str) -> None:
        with mock.patch("sys.stdout", new_callable=StringIO) as mock_stdout:
            method()
            self.assertTrue(re.compile(expected_output).match(mock_stdout.getvalue().strip()), f"Didn't match: {expected_output}")

    @skip_on_windows_until_github_issue_11_is_resolved
    def test_decorator(self) -> None:
        Logging.init_logging()

        def slept_for_approx_25ms_msg(msg_prefix: str) -> str:
            return f"{msg_prefix} took 0.2(5|6|7|8|9) seconds"

        self._assert_method_timing_printed_correctly(self.decorated_method, slept_for_approx_25ms_msg("decorated_method"))
        self._assert_method_timing_printed_correctly(self.decorated_method_with_string_description, slept_for_approx_25ms_msg("String Description"))
        self._assert_method_timing_printed_correctly(self.decorated_method_with_callable_description, slept_for_approx_25ms_msg("DECORATED_METHOD_WITH_CALLABLE_DESCRIPTION"))
        self._assert_method_timing_printed_correctly(lambda: self.decorated_method_takes_params("arg1", 123), slept_for_approx_25ms_msg("decorated_method_takes_params"))

        with CaptureLogs() as log_context:
            self.decorated_method_with_custom_output_method()

            log_lines = log_context.pop_captured_records().with_levels_in({LogLevel.warn}).get_lines(timestamp=False, level=False)

            self.assertEqual(1, len(log_lines))
            self.assertEqual("decorated_method_with_custom_output_method took 0.25 seconds", log_lines[0])

    def test_decorator_name_and_docstring_are_correct(self) -> None:
        self.assertEqual("decorated_method_with_docstring", self.decorated_method_with_docstring.__name__)
        self.assertEqual("This is the docstring", self.decorated_method_with_docstring.__doc__)
