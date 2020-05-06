import logging
import re
import time
from typing import List, Optional, TypeVar
from unittest import TestCase

from puma.attribute import child_only, child_scope_value
from puma.buffer import Observable, Subscriber
from puma.helpers.string import safe_str
from puma.helpers.testing.logging.capture_logs import CaptureLogs
from puma.helpers.testing.parameterized import parameterized
from puma.runnable import SingleBufferServicingRunnable
from tests.buffer._parameterisation import envs
from tests.runnable.test_support.parameterisation import RunnerTestParams
from tests.runnable.test_support.publishing_runnable import PublishingRunnable, PublishingRunnableMode
from tests.runnable.test_support.testval import TestVal

Type = TypeVar("Type")

BUFFER_SIZE = 10
COUNT = BUFFER_SIZE * 3
FAIL_TIMEOUT = 20.0
TEST_TIMEOUT = 20.0

logger = logging.getLogger(__name__)


class EndingConsumerRunnable(SingleBufferServicingRunnable[TestVal], Subscriber[TestVal]):
    _end_at: int = child_only("_end_at")
    _delay: float = child_only("_delay")
    _count: int = child_only("_count")

    def __init__(self, observable: Observable[TestVal], end_at: int, delay: float) -> None:
        super().__init__(observable, self, [], "popper")
        self._end_at = child_scope_value(end_at)
        self._delay = child_scope_value(delay)
        self._count = child_scope_value(0)

    def on_value(self, value: TestVal) -> None:
        logger.debug("Popper got %d", value.counter)
        self._count += 1
        if self._count == self._end_at:
            self._stop_task = True
        time.sleep(self._delay)

    def on_complete(self, error: Optional[BaseException]) -> None:
        if error:
            raise error

    def _execution_ending_hook(self, error: Optional[Exception]) -> bool:
        logger.debug("Popper ending (with error: %s)", safe_str(error))
        return False


class EndWithItemsInBufferSlowTest(TestCase):

    @parameterized(envs)
    def test_with_small_items(self, param: RunnerTestParams) -> None:
        # Close down the system with items still in the buffer. It should end cleanly.
        env = param._env
        with CaptureLogs() as log_context:  # noqa: F841
            with env.create_buffer(TestVal, BUFFER_SIZE, "Publish buffer") as publish_buffer:
                with env.create_runner(PublishingRunnable(COUNT * 1000, publish_buffer, mode=PublishingRunnableMode.PushHard,
                                                          send_complete=False, large_objects=False), "Publisher") as publisher, \
                        env.create_runner(EndingConsumerRunnable(publish_buffer, COUNT, 0.05), "Consumer") as consumer:
                    consumer.start()
                    consumer.wait_until_running(FAIL_TIMEOUT)

                    publisher.start_blocking()  # start this only when everything else is ready

                    test_fail_time = time.monotonic() + TEST_TIMEOUT
                    while consumer.is_alive():
                        publisher.check_for_exceptions()
                        consumer.check_for_exceptions()
                        time.sleep(0.05)
                        self.assertLess(time.monotonic(), test_fail_time, "Deadlock: Test did not end")

                    publisher.stop()

                    consumer.join(FAIL_TIMEOUT)
                    self.assertFalse(consumer.is_alive())
                    publisher.join(FAIL_TIMEOUT)
                    self.assertFalse(publisher.is_alive())

            # # This should work - see issue #221
            # warnings = log_context.pop_captured_records().with_levels_in({LogLevel.warn}).get_lines()
            # self._ensure_discard_warning_present(warnings)

    @parameterized(envs)
    def test_with_large_items(self, param: RunnerTestParams) -> None:
        # Regression test for issue #192: The system is deadlocking on exit with large objects, even though it has been keeping up with transporting them
        # Close down the system with items still in the buffer. It should end cleanly.
        env = param._env
        with CaptureLogs() as log_context:  # noqa: F841
            with env.create_buffer(TestVal, BUFFER_SIZE, "Publish buffer") as publish_buffer:
                with env.create_runner(PublishingRunnable(COUNT * 1000, publish_buffer, mode=PublishingRunnableMode.PushHard,
                                                          send_complete=False, large_objects=True), "Publisher") as publisher, \
                        env.create_runner(EndingConsumerRunnable(publish_buffer, COUNT, 0.05), "Consumer") as consumer:
                    consumer.start()
                    consumer.wait_until_running(FAIL_TIMEOUT)

                    publisher.start_blocking()  # start this only when everything else is ready

                    test_fail_time = time.monotonic() + TEST_TIMEOUT
                    while consumer.is_alive():
                        publisher.check_for_exceptions()
                        consumer.check_for_exceptions()
                        time.sleep(0.05)
                        self.assertLess(time.monotonic(), test_fail_time, "Deadlock: Test did not end")

                    publisher.stop()

                    consumer.join(FAIL_TIMEOUT)
                    self.assertFalse(consumer.is_alive())
                    publisher.join(FAIL_TIMEOUT)
                    self.assertFalse(publisher.is_alive())

            # # This should work - see issue #221
            # warnings = log_context.pop_captured_records().with_levels_in({LogLevel.warn}).get_lines()
            # self._ensure_discard_warning_present(warnings)

    def _ensure_discard_warning_present(self, warnings: List[str]) -> None:
        for warning in warnings:
            if re.search("Publish buffer .*: Discarded .* items from the buffer", warning):
                return
        self.fail(f"The expected discard warning was not found. Warnings were: {warnings}")
