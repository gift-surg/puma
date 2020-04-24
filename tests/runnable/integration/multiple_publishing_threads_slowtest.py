import logging
import time
from typing import TypeVar
from unittest import TestCase

from puma.buffer import MultiThreadBuffer
from puma.runnable.runner import ThreadRunner
from tests.runnable.test_support.publishing_runnable import PublishingRunnable
from tests.runnable.test_support.testval import TestVal
from tests.runnable.test_support.validating_multiple_subscriptions_runnable import ValidatingMultipleSubscriptionsRunnable
from tests.test_logging_helpers import assert_no_warnings_or_errors_logged

Type = TypeVar("Type")

logger = logging.getLogger(__name__)

BUFFER_SIZE = 10
COUNT = BUFFER_SIZE * 3
DELAY = 0.02
FAIL_TIMEOUT = 10.0
TEST_TIMEOUT = 30.0


# A simple multi-threaded test case intended to verify the integration of buffers, runners and runnables


class MultiplePublishingThreadsSlowTest(TestCase):
    @assert_no_warnings_or_errors_logged
    def test(self) -> None:
        with MultiThreadBuffer[TestVal](BUFFER_SIZE, "Publish buffer 1") as publish_buffer_1, \
                MultiThreadBuffer[TestVal](BUFFER_SIZE, "Publish buffer 2") as publish_buffer_2:
            with ThreadRunner(PublishingRunnable(COUNT, publish_buffer_1, delay=DELAY), "Publisher 1") as publish_thread_1, \
                    ThreadRunner(PublishingRunnable(COUNT, publish_buffer_2, delay=DELAY), "Publisher 2") as publish_thread_2, \
                    ThreadRunner(ValidatingMultipleSubscriptionsRunnable([publish_buffer_1, publish_buffer_2], COUNT, "subscriber"), "Consumer") as subscriber_thread:
                test_fail_time = time.monotonic() + TEST_TIMEOUT
                logger.debug("Starting")
                subscriber_thread.start()
                subscriber_thread.wait_until_running(FAIL_TIMEOUT)
                logger.debug("Subscriber started")

                publish_thread_1.start()  # start these only when everything else is ready
                publish_thread_2.start()
                publish_thread_1.wait_until_running(FAIL_TIMEOUT)
                publish_thread_2.wait_until_running(FAIL_TIMEOUT)
                logger.debug("Producers started")

                while publish_thread_1.is_alive() or publish_thread_2.is_alive() or subscriber_thread.is_alive():
                    publish_thread_1.check_for_exceptions()
                    publish_thread_2.check_for_exceptions()
                    subscriber_thread.check_for_exceptions()
                    time.sleep(0.5)
                    self.assertLess(time.monotonic(), test_fail_time, "Deadlock: Test did not end")
