from typing import List
from unittest import TestCase

from puma.runnable.remote_execution.timestamp_tracker import TimestampTracker

FIXED_NOW = 10
CATCH_ALL_TIMESTAMPS = 1000


class TimestampTrackerTest(TestCase):

    def _get_all_timestamps(self, tracker: TimestampTracker) -> List[float]:
        return tracker.entries_in_last_n_seconds(CATCH_ALL_TIMESTAMPS, now_timestamp=FIXED_NOW)

    def _assert_lists_equal(self, expected_list: List, actual_list: List) -> None:

        expected_length = len(expected_list)
        actual_length = len(actual_list)

        if expected_length != actual_length:
            raise AssertionError(f"Lists are different lengths - expected {expected_list} but got {actual_list}")

        for i in range(max(expected_length, actual_length)):
            if abs(expected_list[i] - actual_list[i]) > 0.01:
                raise AssertionError(f"List value at index {i} is not equal - expected {expected_list[i]} but got {actual_list[i]}")

    def test_size_is_respected(self) -> None:
        t = TimestampTracker(3)

        # Check when not full
        t.record(1)
        self._assert_lists_equal([1], self._get_all_timestamps(t))

        # Check with two items
        t.record(2)
        self._assert_lists_equal([1, 2], self._get_all_timestamps(t))

        # Check when full
        t.record(3)
        self._assert_lists_equal([1, 2, 3], self._get_all_timestamps(t))

        # Check that values loop round as expected
        t.record(4)
        t.record(5)
        self._assert_lists_equal([3, 4, 5], self._get_all_timestamps(t))

        # Check that values loop round twice
        t.record(6)
        t.record(7)
        self._assert_lists_equal([5, 6, 7], self._get_all_timestamps(t))

        # ...three times
        t.record(9)
        t.record(10)
        t.record(11)
        self._assert_lists_equal([9, 10, 11], self._get_all_timestamps(t))

        # ..and five
        t.record(12)
        t.record(13)
        t.record(14)
        t.record(15)
        t.record(16)
        self._assert_lists_equal([14, 15, 16], self._get_all_timestamps(t))

        # Final check to ensure that entries are still returned in the correct order
        t.record(17)
        t.record(18)
        self._assert_lists_equal([16, 17, 18], self._get_all_timestamps(t))

    def test_check_get_last_entries_works(self) -> None:
        t = TimestampTracker(3)

        # Check when not full
        t.record(1)
        self._assert_lists_equal([1], t.entries_in_last_n_seconds(5, now_timestamp=5))
        self._assert_lists_equal([], t.entries_in_last_n_seconds(5, now_timestamp=6.1))

        # Check with two items
        t.record(2)
        self._assert_lists_equal([1, 2], t.entries_in_last_n_seconds(5, now_timestamp=5))
        self._assert_lists_equal([2], t.entries_in_last_n_seconds(5, now_timestamp=6.1))

        # Check when full
        t.record(3)
        self._assert_lists_equal([1, 2, 3], t.entries_in_last_n_seconds(5, now_timestamp=5))
        self._assert_lists_equal([2, 3], t.entries_in_last_n_seconds(5, now_timestamp=6.1))

        # Check that values loop round as expected
        t.record(4)
        t.record(5)
        self._assert_lists_equal([3, 4, 5], t.entries_in_last_n_seconds(5, now_timestamp=5))
        self._assert_lists_equal([4, 5], t.entries_in_last_n_seconds(1.9, now_timestamp=5))
        self._assert_lists_equal([5], t.entries_in_last_n_seconds(0.9, now_timestamp=5))
        self._assert_lists_equal([], t.entries_in_last_n_seconds(0.9, now_timestamp=6))

        # ...loop round a few more times
        t.record(6)
        t.record(7)
        t.record(8)
        t.record(9)
        t.record(10)
        self._assert_lists_equal([8, 9, 10], t.entries_in_last_n_seconds(5, now_timestamp=12))
        self._assert_lists_equal([9, 10], t.entries_in_last_n_seconds(3.9, now_timestamp=12))
        self._assert_lists_equal([10], t.entries_in_last_n_seconds(2.9, now_timestamp=12))
        self._assert_lists_equal([], t.entries_in_last_n_seconds(1.9, now_timestamp=12))
