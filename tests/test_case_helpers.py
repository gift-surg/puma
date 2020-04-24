import statistics
from typing import Any, Collection, Iterable, List, Optional, Sized, Tuple, Union, cast
from unittest import TestCase


def is_empty(things: Union[Sized, Iterable[Any]]) -> bool:
    """Returns true if the given container is empty."""

    length_func = getattr(things, '__len__', None)
    if length_func is not None and callable(length_func):
        return int(length_func()) == 0
    else:
        it = iter(cast(Iterable, things))
        try:
            next(it)
        except StopIteration:
            return True
        else:
            return False


def is_not_empty(things: Union[Sized, Iterable[Any]]) -> bool:
    """Returns true if the given container is not empty."""

    return not is_empty(things)


# noinspection PyPep8Naming
def assertEmpty(testcase: TestCase, things: Union[Sized, Iterable[Any]], msg: Optional[str] = None) -> None:
    """Raises an assertion error if the given item is not empty."""

    if not isinstance(testcase, TestCase):
        raise TypeError("testcase parameter must be an instance of TestCase")
    testcase.assertTrue(is_empty(things), msg)


# noinspection PyPep8Naming
def assertNotEmpty(testcase: TestCase, things: Union[Sized, Iterable[Any]], msg: Optional[str] = None) -> None:
    """Raises an assertion error if the given item is empty."""

    if not isinstance(testcase, TestCase):
        raise TypeError("testcase parameter must be an instance of TestCase")
    testcase.assertFalse(is_empty(things), msg)


def calculate_intervals(tick_times: List[float]) -> List[float]:
    """Given a list of times, return a list of the intervals between those times"""
    return [tick_times[i] - tick_times[i - 1] for i in range(1, len(tick_times))]


def calc_lenient_interval_stats(all_intervals: Collection[float]) -> Tuple[float, float, float]:
    """Calculate min, max and mean values for the given time intervals, in a lenient manner (discarding outliers)"""

    # We need to be a little lenient on the min and max intervals: we are trying to achieve a constant rate rather than exact intervals.
    # To achieve this leniency, I'm going to sort the intervals and discard outliers (the highest and lowest 5% of values)
    discard_count = max(1, len(all_intervals) // 20)
    sorted_intervals = sorted(all_intervals)
    intervals = sorted_intervals[discard_count:-discard_count]
    min_interval = intervals[0]
    max_interval = intervals[-1]
    mean_interval = statistics.mean(intervals)
    return max_interval, mean_interval, min_interval
