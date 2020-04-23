import time
from threading import Thread
from unittest import TestCase

from puma.primitives import HighPrecisionCondition
from tests.github_issue_11_quickfix import skip_on_windows_until_github_issue_11_is_resolved

TIMEOUTS = [0.0, 0.000001, 0.014, 0.015, 0.016, 0.017, 0.018, 0.3]
PRECISION = 0.005
TIMEOUT = 0.3


class HighPrecisionConditionTest(TestCase):
    # HighPrecisionCondition is derived from threading.Condition, and only overrides the wait() method. We only test the modified behaviour, not the base class's behaviour.

    @skip_on_windows_until_github_issue_11_is_resolved
    def test_if_times_out(self) -> None:
        self._test_wait_when_time_out(0.1)

    def test_if_notified_with_timeout(self) -> None:
        condition = HighPrecisionCondition()
        thread = Thread(target=self._sleep_then_notify, args=[TIMEOUT, condition])
        thread.start()
        try:
            with condition:
                t1 = time.perf_counter()
                ret = condition.wait(TIMEOUT * 3)  # timout used here should not happen: condition should be raised after TIMEOUT
                t2 = time.perf_counter()
                self.assertTrue(ret)
                self.assertGreaterEqual(t2 - t1, TIMEOUT)  # Check the wait did not return until the condition was raised
                self.assertLess(t2 - t1, TIMEOUT * 2)  # Check that the wait did return early (when the condition was raised)
        finally:
            thread.join()

    def test_if_notified_timeout_is_none(self) -> None:
        condition = HighPrecisionCondition()
        thread = Thread(target=self._sleep_then_notify, args=[TIMEOUT, condition])
        thread.start()
        try:
            with condition:
                t1 = time.perf_counter()
                ret = condition.wait(None)
                t2 = time.perf_counter()
                self.assertTrue(ret)
                self.assertGreaterEqual(t2 - t1, TIMEOUT)  # Check the wait did not return until the condition was raised
                self.assertLess(t2 - t1, TIMEOUT * 2)  # Crude sanity check in case some default timeout was (wrongly) implemented
        finally:
            thread.join()

    @skip_on_windows_until_github_issue_11_is_resolved
    def test_wait_timeout_precision(self) -> None:
        for timeout in TIMEOUTS:
            time.sleep(0.25)  # Try to reduce load on CPU so that test is less likely to fail on heavily loaded machine
            self._test_wait_when_time_out(timeout)

    @skip_on_windows_until_github_issue_11_is_resolved
    def test_wait_for_timeout_precision(self) -> None:
        for timeout in TIMEOUTS:
            time.sleep(0.25)  # Try to reduce load on CPU so that test is less likely to fail on heavily loaded machine
            self._test_wait_for_when_time_out(timeout)

    def _test_wait_when_time_out(self, timeout: float) -> None:
        condition = HighPrecisionCondition()
        with condition:
            t1 = time.perf_counter()
            ret = condition.wait(timeout)
            t2 = time.perf_counter()
            self.assertFalse(ret)
            self.assertGreaterEqual(t2 - t1, timeout, f"Took {t2 - t1} when timeout was {timeout}")
            self.assertLess(t2 - t1, timeout + PRECISION, f"Took {t2 - t1} when timeout was {timeout}")

    def _test_wait_for_when_time_out(self, timeout: float) -> None:
        condition = HighPrecisionCondition()
        with condition:
            t1 = time.perf_counter()
            ret = condition.wait_for(lambda: False, timeout)
            t2 = time.perf_counter()
            self.assertFalse(ret)
            self.assertGreaterEqual(t2 - t1, timeout, f"Took {t2 - t1} when timeout was {timeout}")
            self.assertLess(t2 - t1, timeout + PRECISION, f"Took {t2 - t1} when timeout was {timeout}")

    @staticmethod
    def _sleep_then_notify(delay: float, condition: HighPrecisionCondition) -> None:
        time.sleep(delay)
        with condition:
            condition.notify()
