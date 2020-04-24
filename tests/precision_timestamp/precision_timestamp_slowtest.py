import random
import statistics
import time
from concurrent.futures import Executor, ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor
from typing import List
from unittest import TestCase

from puma.precision_timestamp.precision_timestamp import precision_timestamp
from tests.test_case_helpers import calc_lenient_interval_stats, calculate_intervals

COUNT = 500
INTERVAL = 0.01
MEAN_PRECISION = 0.0005
PERMITTED_TIME_DELTA = 0.0005


class PrecisionTimestampSlowtest(TestCase):
    def test_precision(self) -> None:
        t1 = time.perf_counter()
        times = self._get_times(count=COUNT, poll_period=INTERVAL)
        t2 = time.perf_counter()
        loop_interval = (t2 - t1) / COUNT  # should be very similar to INTERVAL, but may be a little longer because we use sleep() plus looping to time the interval

        all_intervals = calculate_intervals(times)

        max_interval, mean_interval, min_interval = calc_lenient_interval_stats(all_intervals)

        # print(f"min interval: {min_interval}, max interval: {max_interval}, mean interval: {mean_interval}")

        # The interval can be less than the nominal interval when one tick is late and the next is on time.
        self.assertGreaterEqual(min_interval, loop_interval / 2, f"Min interval too low ({min_interval}). Intervals are {all_intervals}")
        self.assertLessEqual(min_interval, loop_interval * 3 / 2, f"Min interval too high ({min_interval}). Intervals are {all_intervals}")
        self.assertGreaterEqual(max_interval, loop_interval / 2, f"Max interval too low ({max_interval}). Intervals are {all_intervals}")
        self.assertLessEqual(max_interval, loop_interval * 3 / 2, f"Max interval too high ({max_interval}). Intervals are {all_intervals}")
        self.assertGreaterEqual(mean_interval, loop_interval - MEAN_PRECISION, f"Mean interval too low ({mean_interval}). Intervals are {all_intervals}")
        self.assertLessEqual(mean_interval, loop_interval + MEAN_PRECISION, f"Mean interval too high ({mean_interval}). Intervals are {all_intervals}")

    def test_absolute_values_same_in_threads(self) -> None:
        executor = ThreadPoolExecutor(max_workers=2)
        self._test_absolute_values_same(executor)

    def test_absolute_values_same_in_processes(self) -> None:
        executor = ProcessPoolExecutor(max_workers=2)
        self._test_absolute_values_same(executor)

    def _test_absolute_values_same(self, executor: Executor) -> None:
        # Start several threads or processes, at staggered intervals.
        # In each thread/process, find the mean difference between the precision_timestamp() and time.time().
        # This difference should be the same in each thread/process, providing that the machine's "wall time" is not changed during the test, which is unlikely.
        futures = [executor.submit(self._get_wall_clock_diff) for _ in range(0, 10)]
        differences = [future.result() for future in futures]

        # check the range between the minimum and maximum offsets
        differences.sort()
        diff_range = abs(differences[0] - differences[len(differences) - 1])
        self.assertAlmostEqual(0.0, diff_range, delta=PERMITTED_TIME_DELTA)

    @staticmethod
    def _random_delay() -> None:
        time.sleep(random.uniform(0.0, 1.0))

    @staticmethod
    def _get_wall_clock_diff() -> float:
        PrecisionTimestampSlowtest._random_delay()
        return statistics.mean(PrecisionTimestampSlowtest._get_diffs_with_wall_time(100, 0.005))

    @staticmethod
    def _get_times(count: int, poll_period: float) -> List[float]:
        ret: List[float] = []
        for i in range(count):
            ret.append(precision_timestamp())
            time.sleep(poll_period)
        return ret

    @staticmethod
    def _get_diffs_with_wall_time(count: int, poll_period: float) -> List[float]:
        ret: List[float] = []
        for i in range(count):
            ret.append(precision_timestamp() - time.time())
            time.sleep(poll_period)
        return ret
