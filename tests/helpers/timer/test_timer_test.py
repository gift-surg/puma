from time import sleep
from unittest import TestCase

from tests.github_issue_11_quickfix import skip_on_windows_until_github_issue_11_is_resolved
from tests.helpers.timer.test_timer import TestTimer


class TestTimerTest(TestCase):

    @skip_on_windows_until_github_issue_11_is_resolved
    def test_assert_elapsed_time_equals_works(self) -> None:
        with TestTimer(self) as timer_1:
            sleep(0.4)
            with timer_1.sub_timer() as timer_2:
                sleep(0.2)
                timer_2.assert_elapsed_time_equals(0.2)
                with timer_2.sub_timer() as timer_3:
                    sleep(0.3)
                    timer_3.assert_elapsed_time_equals(0.3)
                timer_2.assert_elapsed_time_equals(0.5)
            timer_1.assert_elapsed_time_equals(0.9)

            # Ensure it fails when expected
            with self.assertRaisesRegex(AssertionError, "!= 0.1 within 2 places (.*) Elapsed time was not equal to expected"):
                timer_1.assert_elapsed_time_equals(0.1)

    def test_assert_elapsed_time_less_than_works(self) -> None:
        with TestTimer(self) as timer_1:
            sleep(0.4)
            timer_1.assert_elapsed_time_less_than(0.5)

            # Ensure it fails when expected
            with self.assertRaisesRegex(AssertionError, "not less than 0.2 : Elapsed time was not less than expected"):
                timer_1.assert_elapsed_time_less_than(0.2)

    def test_assert_elapsed_time_greater_than_works(self) -> None:
        with TestTimer(self) as timer_1:
            sleep(0.4)
            timer_1.assert_elapsed_time_greater_than(0.2)

            # Ensure it fails when expected
            with self.assertRaisesRegex(AssertionError, "not greater than 0.5 : Elapsed time was not greater than expected"):
                timer_1.assert_elapsed_time_greater_than(0.5)

    def test_assert_elapsed_time_within_range_works(self) -> None:
        with TestTimer(self) as timer:
            sleep(0.5)
            timer.assert_elapsed_time_within_range(0.4, 0.6)

            # Ensure it fails when expected
            with self.assertRaisesRegex(AssertionError, r"Elapsed time \([\d\.]+\) was not within the expected range \(0\.1 \- 0\.4\)"):
                timer.assert_elapsed_time_within_range(0.1, 0.4)

            with self.assertRaisesRegex(AssertionError, r"Elapsed time \([\d\.]+\) was not within the expected range \(0\.6 \- 1\)"):
                timer.assert_elapsed_time_within_range(0.6, 1)
