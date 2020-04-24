from unittest import TestCase

from puma.helpers.string import safe_str
from tests.test_logging_helpers import assert_no_warnings_or_errors_logged


class StringHelperSafeStrTest(TestCase):
    @assert_no_warnings_or_errors_logged
    def test_usual_cases(self) -> None:
        self.assertEqual("123", safe_str(123))
        self.assertEqual("abc", safe_str("abc"))
        self.assertEqual("{1: 123}", safe_str({1: 123}))

    @assert_no_warnings_or_errors_logged
    def test_falsy_cases(self) -> None:
        # "falsy" values (e.g. zero, empty string) must return their actual value, not "None"
        self.assertEqual("0", safe_str(0))
        self.assertEqual("", safe_str(""))

    @assert_no_warnings_or_errors_logged
    def test_none_case(self) -> None:
        self.assertEqual("None", safe_str(None))

    @assert_no_warnings_or_errors_logged
    def test_exception_case(self) -> None:
        self.assertEqual("RuntimeError('Test')", safe_str(RuntimeError("Test")))

    @assert_no_warnings_or_errors_logged
    def test_exception_in_str_method_case(self) -> None:
        self.assertEqual("<ERROR>: Foo", safe_str(StringHelperSafeStrTest.ErrorInStr()))

    class ErrorInStr:
        def __str__(self) -> str:
            raise ValueError("Foo")
