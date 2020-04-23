import logging
import time
from typing import TypeVar
from unittest import TestCase

from tests.environment.parameterisation import EnvironmentTestParameters, environments
from tests.parameterized import parameterized
from tests.runnable.test_support.publishing_runnable import PublishingRunnable
from tests.runnable.test_support.testval import TestVal
from tests.runnable.test_support.validating_single_subscription_runnable import ValidatingSingleSubscriptionRunnable
from tests.test_logging_helpers import assert_no_warnings_or_errors_logged

Type = TypeVar("Type")

logger = logging.getLogger(__name__)

BUFFER_SIZE = 10
COUNT = BUFFER_SIZE * 3
DELAY = 0.02
FAIL_TIMEOUT = 10.0
TEST_TIMEOUT = 30.0


# A simple integration test for buffers, runners and runnables in both multi-thread and multi-process scenarios, in the case where the data publisher
# is slower than the consumer so the buffers are typically empty.


class PublisherConsumerPushLimitedSlowTest(TestCase):
    @parameterized(environments)
    @assert_no_warnings_or_errors_logged
    def test(self, param: EnvironmentTestParameters) -> None:
        env = param.environment
        with env.create_buffer(TestVal, BUFFER_SIZE, "Publish buffer") as publish_buffer:
            with env.create_runner(PublishingRunnable(COUNT, publish_buffer, delay=DELAY), "Publisher") as publisher, \
                    env.create_runner(ValidatingSingleSubscriptionRunnable(publish_buffer, COUNT), "Consumer") as consumer:
                test_fail_time = time.monotonic() + TEST_TIMEOUT
                logger.debug("Starting")
                consumer.start()
                consumer.wait_until_running(FAIL_TIMEOUT)
                logger.debug("Consumer started")

                publisher.start_blocking()  # start this only when everything else is ready
                logger.debug("Publisher started")

                while publisher.is_alive() or consumer.is_alive():
                    publisher.check_for_exceptions()
                    consumer.check_for_exceptions()
                    time.sleep(0.5)
                    self.assertLess(time.monotonic(), test_fail_time, "Deadlock: Test did not end")
