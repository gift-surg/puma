from unittest import TestCase

from puma.context import ensure_used_within_context_manager
from puma.helpers.testing.mixin import NotATestCase
from puma.helpers.timer import Timer


class TestTimer(Timer, NotATestCase):
    """A ContextManager for testing the time taken for a section of code to complete"""

    def __init__(self, test_case: TestCase) -> None:
        super().__init__()
        self._test_case = test_case

    def __enter__(self) -> "TestTimer":
        super().__enter__()
        return self

    @ensure_used_within_context_manager
    def sub_timer(self) -> "TestTimer":
        return TestTimer(self._test_case)

    @ensure_used_within_context_manager
    def assert_elapsed_time_equals(self, expected_elapsed_time: float) -> None:
        self._test_case.assertAlmostEqual(self.get_elapsed_time(), expected_elapsed_time, 2, msg="Elapsed time was not equal to expected")

    @ensure_used_within_context_manager
    def assert_elapsed_time_less_than(self, expected_elapsed_time: float) -> None:
        self._test_case.assertLess(self.get_elapsed_time(), expected_elapsed_time, msg="Elapsed time was not less than expected")

    @ensure_used_within_context_manager
    def assert_elapsed_time_greater_than(self, expected_elapsed_time: float) -> None:
        self._test_case.assertGreater(self.get_elapsed_time(), expected_elapsed_time, msg="Elapsed time was not greater than expected")

    @ensure_used_within_context_manager
    def assert_elapsed_time_within_range(self, range_min: float, range_max: float) -> None:
        try:
            self.assert_elapsed_time_greater_than(range_min)
            self.assert_elapsed_time_less_than(range_max)
        except AssertionError as ae:
            raise AssertionError(f"Elapsed time ({self.get_elapsed_time()}) was not within the expected range ({range_min} - {range_max})") from ae
