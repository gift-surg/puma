import functools
from typing import Any, Callable, TypeVar, Union, overload
from unittest import TestCase

from puma.context import Exit_1, Exit_2, Exit_3
from puma.logging import LogLevel
from tests.logging.capture_logs import CaptureLogs

RT = TypeVar('RT')  # _return type


@overload  # noqa: F811
def assert_no_warnings_or_errors_logged(func_or_testcase: Callable[..., RT]) -> Callable[..., RT]:
    """A decorator that checks that no errors or warnings are logged within the scope.

    For this to work, a logger must be configured at warning level to capture the log messages -  this will usually be the case since default logging
    is at warning level.

    @assert_no_warnings_or_errors_logged
    def do_something():
        ...
    """
    ...


@overload  # noqa: F811
def assert_no_warnings_or_errors_logged(func_or_testcase: TestCase) -> 'AssertingCaptureLogs':
    """A function returning a context managed object which checks that no errors or warnings are logged within the scope.

    For this to work, a logger must be configured at warning level to capture the log messages -  this will usually be the case since default logging
    is at warning level.

    def do_something():
        assert_no_warnings_or_errors_logged(self) as log_context:
            ...
    """
    ...


def assert_no_warnings_or_errors_logged(func_or_testcase: Union[Callable[..., RT], TestCase]) -> Union[Callable[..., RT], 'AssertingCaptureLogs']:  # noqa: F811
    """Implementation of the assert_no_warnings_or_errors_logged overloads."""
    if isinstance(func_or_testcase, TestCase):
        return AssertingCaptureLogs(func_or_testcase)
    elif callable(func_or_testcase):
        return assert_no_warnings_or_errors_logged_decorator(func_or_testcase)
    else:
        raise TypeError("assert_no_warnings_or_errors_logged called with unsupported parameter type")


def assert_no_warnings_or_errors_logged_decorator(func: Callable[..., RT]) -> Callable[..., RT]:
    """A decorator that can be applied to test methods (i.e. methods of classes derived from TestCase). Causes a failure if the method logs any warning or error messages."""

    @functools.wraps(func)
    def wrapper(self: TestCase, *args: Any, **kwargs: Any) -> RT:
        with AssertingCaptureLogs(self):
            return func(self, *args, **kwargs)

    return wrapper


class AssertingCaptureLogs(CaptureLogs):
    def __init__(self, test_case: TestCase) -> None:
        self._test_case = test_case
        super().__init__(LogLevel.warn)

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        records = self._context.pop_captured_records()
        super().__exit__(exc_type, exc_value, traceback)
        filtered_records = records.with_levels_in({LogLevel.warn, LogLevel.error, LogLevel.fatal})
        if filtered_records:
            lines = filtered_records.get_lines(timestamp=False)
            self._test_case.fail(f"Unexpectedly raised warnings or errors: {lines}")
