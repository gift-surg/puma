import logging
import time
from typing import TypeVar
from unittest import TestCase

from puma.buffer import MultiThreadBuffer
from puma.helpers.string import safe_str
from puma.runnable.runner import ThreadRunner
from tests.runnable.test_support.publishing_runnable import PublishingRunnable
from tests.runnable.test_support.testval import TestVal
from tests.runnable.test_support.validating_single_subscription_runnable import ValidatingSingleSubscriptionRunnable

Type = TypeVar("Type")

logger = logging.getLogger(__name__)

BUFFER_SIZE = 10
COUNT = BUFFER_SIZE * 3
DELAY = 0.02
FAIL_TIMEOUT = 10.0
TEST_TIMEOUT = 30.0


# Tests that an error raised in a source runnable appears at the subscribing runnable


class ErroringThreadsSlowTest(TestCase):
    def test(self) -> None:
        # This is the actual test case!
        with self.assertRaisesRegex(RuntimeError, "Test Error"):
            self._test_impl()

    def _test_impl(self) -> None:
        # This method is called by the test case.  The line 'subscriber_thread.check_for_exceptions()' below should eventually raise an exception, though it may
        # not do so the first time it is called.
        with MultiThreadBuffer[TestVal](BUFFER_SIZE, "Publish buffer") as publish_buffer:
            with ThreadRunner(PublishingRunnable(COUNT, publish_buffer, delay=DELAY, do_error=True), "Publisher") as publish_thread, \
                    ThreadRunner(ValidatingSingleSubscriptionRunnable(publish_buffer, COUNT, should_receive_all=False, raise_received_completion_errors=True),
                                 "Consumer") as subscriber_thread:
                test_fail_time = time.monotonic() + TEST_TIMEOUT
                logger.debug("Starting")
                subscriber_thread.start()
                subscriber_thread.wait_until_running(FAIL_TIMEOUT)
                logger.debug("Subscriber started")

                publish_thread.start_blocking()  # start this only when everything else is ready
                logger.debug("Publisher started")

                while subscriber_thread.is_alive():
                    try:
                        publish_thread.check_for_exceptions()  # This should NOT throw exceptions - the publisher should pass its errors to its subscriber
                    except Exception as ex:
                        if isinstance(ex, RuntimeError) and str(ex) == "Test Error":
                            self.fail("Test failure: Publishing runnable should have passed its error to its subscriber, not raised it.")
                        else:
                            self.fail(f"Publishing thread raised an exception: {safe_str(ex)}")
                    subscriber_thread.check_for_exceptions()  # This SHOULD throw exceptions - received from the publishing runnable
                    time.sleep(0.5)
                    self.assertLess(time.monotonic(), test_fail_time, "Deadlock: Test did not end")
