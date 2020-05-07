from unittest import TestCase

from puma.helpers.string import list_str
from puma.helpers.testing.logging.decorator import assert_no_warnings_or_errors_logged


class _Thing:
    def __str__(self) -> str:
        return "thingy"


class StringHelperListStrTest(TestCase):
    @assert_no_warnings_or_errors_logged
    def test_none_list(self) -> None:
        self.assertEqual("None", list_str(None))

    @assert_no_warnings_or_errors_logged
    def test_empty(self) -> None:
        self.assertEqual("[]", list_str([]))

    @assert_no_warnings_or_errors_logged
    def test_single_item(self) -> None:
        self.assertEqual("[1]", list_str([1]))

    @assert_no_warnings_or_errors_logged
    def test_multiple_items(self) -> None:
        self.assertEqual("[1, 2, 3]", list_str([1, 2, 3]))

    @assert_no_warnings_or_errors_logged
    def test_str_is_called(self) -> None:
        self.assertEqual("[thingy]", list_str([_Thing()]))

    @assert_no_warnings_or_errors_logged
    def test_none_element(self) -> None:
        self.assertEqual("[1, None, 3]", list_str([1, None, 3]))

    @assert_no_warnings_or_errors_logged
    def test_strings_are_quoted(self) -> None:
        self.assertEqual("['a', 'b']", list_str(["a", 'b']))

    @assert_no_warnings_or_errors_logged
    def test_mixed_items(self) -> None:
        self.assertEqual("['a', 1, None, thingy]", list_str(["a", 1, None, _Thing()]))
