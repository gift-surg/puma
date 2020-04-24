import time
from typing import Optional
from unittest import TestCase

from puma.attribute import parent_only
from puma.buffer import Observable, Publishable
from puma.helpers.string import safe_str
from puma.runnable import MultiBufferServicingRunnable
from puma.runnable.runner import Runner
from tests.environment.parameterisation import EnvironmentTestParameters, environments
from tests.mixin import NotATestCase
from tests.parameterized import parameterized
from tests.runnable.test_support.forwarding_subscriber import ForwardingSubscriber
from tests.runnable.test_support.publishing_runnable import PublishingRunnable
from tests.runnable.test_support.testval import TestVal
from tests.runnable.test_support.validating_single_subscription_runnable import ValidatingSingleSubscriptionRunnable
from tests.test_logging_helpers import assert_no_warnings_or_errors_logged

BUFFER_SIZE = 10
COUNT = BUFFER_SIZE * 15
PUSH_INTERVAL_1 = 0.02
PUSH_INTERVAL_2 = 0.05
FAIL_TIMEOUT = 10.0

STOP_DELAY = COUNT * min(PUSH_INTERVAL_1, PUSH_INTERVAL_2) / 2
TEST_TIMEOUT = COUNT * max(PUSH_INTERVAL_1, PUSH_INTERVAL_2) * 10


# Test MultiBufferServicingRunnable across threads and processes. Most of the testing of MultiBufferServicingRunnable is done in a single thread, see
# multi_buffer_servicing_runnable_test.py. This 'slow' test ensures that it really works across threads and processes.
# There is a separate test harness for the "ticking" functionality of MultiBufferServicingRunnable.


class TestMultiBufferServicingRunnable(MultiBufferServicingRunnable, NotATestCase):
    _cause_error_at_count: int = parent_only("_cause_error_at_count")

    # Test class for MultiBufferServicingRunnable. Copies values received on each input buffer to a corresponding output buffer.
    def __init__(self, cause_error_at_count: int = -1) -> None:
        super().__init__("Test runnable", [])
        self._cause_error_at_count = cause_error_at_count

    def set_up_forwarding(self, in_buffer: Observable[TestVal], out_buffer: Publishable[TestVal]) -> None:
        self.multicaster_accessor.add_output_buffer(out_buffer)
        publisher = self._get_publisher_unwrapped(out_buffer)
        subscriber = ForwardingSubscriber(in_buffer.buffer_name(), publisher, cause_error_at_count=self._cause_error_at_count)
        self._add_subscription(in_buffer, subscriber)


class MultiBufferServicingRunnableSlowTest(TestCase):
    def setUp(self) -> None:
        self._consumer_1_error: Optional[Exception] = None
        self._consumer_2_error: Optional[Exception] = None

    @parameterized(environments)
    @assert_no_warnings_or_errors_logged
    def test_ends_when_buffers_completed(self, param: EnvironmentTestParameters) -> None:
        env = param.environment
        runnable = TestMultiBufferServicingRunnable()
        with env.create_buffer(TestVal, BUFFER_SIZE, "Publish buffer 1") as publish_buffer_1, \
                env.create_buffer(TestVal, BUFFER_SIZE, "Publish buffer 2") as publish_buffer_2, \
                env.create_buffer(TestVal, BUFFER_SIZE, "Subscriber buffer 1") as subscriber_buffer_1, \
                env.create_buffer(TestVal, BUFFER_SIZE, "Subscriber buffer 2") as subscriber_buffer_2:
            with env.create_runner(PublishingRunnable(COUNT, publish_buffer_1, delay=PUSH_INTERVAL_1), "Publisher 1") as publisher_1, \
                    env.create_runner(PublishingRunnable(COUNT, publish_buffer_2, delay=PUSH_INTERVAL_2), "Publisher 2") as publisher_2, \
                    env.create_runner(ValidatingSingleSubscriptionRunnable(subscriber_buffer_1, COUNT), "Consumer 1") as consumer_1, \
                    env.create_runner(ValidatingSingleSubscriptionRunnable(subscriber_buffer_2, COUNT), "Consumer 2") as consumer_2, \
                    env.create_runner(runnable) as runner:
                test_fail_time = time.monotonic() + TEST_TIMEOUT

                runnable.set_up_forwarding(publish_buffer_1, subscriber_buffer_1)
                runnable.set_up_forwarding(publish_buffer_2, subscriber_buffer_2)
                runner.start()
                consumer_1.start()
                consumer_2.start()
                runner.wait_until_running(FAIL_TIMEOUT)
                consumer_1.wait_until_running(FAIL_TIMEOUT)
                consumer_2.wait_until_running(FAIL_TIMEOUT)
                publisher_1.start()
                publisher_2.start()
                publisher_1.wait_until_running(FAIL_TIMEOUT)
                publisher_2.wait_until_running(FAIL_TIMEOUT)

                while consumer_1.is_alive() or consumer_2.is_alive():
                    publisher_1.check_for_exceptions()
                    publisher_2.check_for_exceptions()
                    consumer_1.check_for_exceptions()
                    consumer_2.check_for_exceptions()
                    runner.check_for_exceptions()
                    time.sleep(0.1)
                    self.assertLess(time.monotonic(), test_fail_time, "Deadlock: Test did not end")

    @parameterized(environments)
    @assert_no_warnings_or_errors_logged
    def test_ends_when_stopped(self, param: EnvironmentTestParameters) -> None:
        env = param.environment
        runnable = TestMultiBufferServicingRunnable()
        with env.create_buffer(TestVal, BUFFER_SIZE, "Publish buffer 1") as publish_buffer_1, \
                env.create_buffer(TestVal, BUFFER_SIZE, "Publish buffer 2") as publish_buffer_2, \
                env.create_buffer(TestVal, BUFFER_SIZE, "Subscriber buffer 1") as subscriber_buffer_1, \
                env.create_buffer(TestVal, BUFFER_SIZE, "Subscriber buffer 2") as subscriber_buffer_2:
            with env.create_runner(PublishingRunnable(COUNT, publish_buffer_1, delay=PUSH_INTERVAL_1, error_if_full=False, send_complete=False),
                                   "Publisher 1") as publisher_1, \
                    env.create_runner(PublishingRunnable(COUNT, publish_buffer_2, delay=PUSH_INTERVAL_2, error_if_full=False, send_complete=False),
                                      "Publisher 2") as publisher_2, \
                    env.create_runner(ValidatingSingleSubscriptionRunnable(subscriber_buffer_1, COUNT, should_receive_all=False),
                                      "Consumer 1") as consumer_1, \
                    env.create_runner(ValidatingSingleSubscriptionRunnable(subscriber_buffer_2, COUNT, should_receive_all=False),
                                      "Consumer 2") as consumer_2, \
                    env.create_runner(runnable) as runner:
                test_fail_time = time.monotonic() + TEST_TIMEOUT

                runnable.set_up_forwarding(publish_buffer_1, subscriber_buffer_1)
                runnable.set_up_forwarding(publish_buffer_2, subscriber_buffer_2)
                runner.start()
                consumer_1.start()
                consumer_2.start()
                runner.wait_until_running(FAIL_TIMEOUT)
                consumer_1.wait_until_running(FAIL_TIMEOUT)
                consumer_2.wait_until_running(FAIL_TIMEOUT)

                publisher_1.start()
                publisher_2.start()
                publisher_1.wait_until_running(FAIL_TIMEOUT)
                publisher_2.wait_until_running(FAIL_TIMEOUT)

                stop_time = time.monotonic() + STOP_DELAY
                while consumer_1.is_alive() or consumer_2.is_alive():
                    publisher_1.check_for_exceptions()
                    publisher_2.check_for_exceptions()
                    consumer_1.check_for_exceptions()
                    consumer_2.check_for_exceptions()
                    runner.check_for_exceptions()
                    time.sleep(0.1)
                    if time.monotonic() > stop_time:
                        runner.stop()
                    self.assertLess(time.monotonic(), test_fail_time, "Deadlock: Test did not end")

    @parameterized(environments)
    def test_when_error(self, param: EnvironmentTestParameters) -> None:
        env = param.environment
        runnable = TestMultiBufferServicingRunnable(cause_error_at_count=COUNT // 2)
        with env.create_buffer(TestVal, BUFFER_SIZE, "Publish buffer 1") as publish_buffer_1, \
                env.create_buffer(TestVal, BUFFER_SIZE, "Publish buffer 2") as publish_buffer_2, \
                env.create_buffer(TestVal, BUFFER_SIZE, "Subscriber buffer 1") as subscriber_buffer_1, \
                env.create_buffer(TestVal, BUFFER_SIZE, "Subscriber buffer 2") as subscriber_buffer_2:
            with env.create_runner(PublishingRunnable(COUNT, publish_buffer_1, delay=PUSH_INTERVAL_1,
                                                      error_if_full=False, send_complete=False), "Publisher 1") as publisher_1, \
                    env.create_runner(PublishingRunnable(COUNT, publish_buffer_2, delay=PUSH_INTERVAL_2,
                                                         error_if_full=False, send_complete=False), "Publisher 2") as publisher_2, \
                    env.create_runner(ValidatingSingleSubscriptionRunnable(subscriber_buffer_1, COUNT, should_receive_all=False, raise_received_completion_errors=True),
                                      "Consumer 1") as consumer_1, \
                    env.create_runner(ValidatingSingleSubscriptionRunnable(subscriber_buffer_2, COUNT, should_receive_all=False, raise_received_completion_errors=True),
                                      "Consumer 2") as consumer_2, \
                    env.create_runner(runnable) as runner:
                test_fail_time = time.monotonic() + TEST_TIMEOUT

                runnable.set_up_forwarding(publish_buffer_1, subscriber_buffer_1)
                runnable.set_up_forwarding(publish_buffer_2, subscriber_buffer_2)
                runner.start()
                consumer_1.start()
                consumer_2.start()
                runner.wait_until_running(FAIL_TIMEOUT)
                consumer_1.wait_until_running(FAIL_TIMEOUT)
                consumer_2.wait_until_running(FAIL_TIMEOUT)

                publisher_1.start()
                publisher_2.start()
                publisher_1.wait_until_running(FAIL_TIMEOUT)
                publisher_2.wait_until_running(FAIL_TIMEOUT)

                while consumer_1.is_alive() or consumer_2.is_alive():
                    self._check_for_exceptions(publisher_1, publisher_2, runner, consumer_1, consumer_2)
                    time.sleep(0.1)
                    self.assertLess(time.monotonic(), test_fail_time, "Deadlock: Test did not end")

                # check for errors again, because we may have stopped before testing; normally this would get raised when the
                # runner exits context management, but here we are trying to catch it to check it
                self._check_for_exceptions(publisher_1, publisher_2, runner, consumer_1, consumer_2)

                self.assertIsNotNone(self._consumer_1_error)
                self.assertIsNotNone(self._consumer_2_error)
                self.assertEqual("RuntimeError('Test Error!')", safe_str(self._consumer_1_error))
                self.assertEqual("RuntimeError('Test Error!')", safe_str(self._consumer_2_error))

    def _check_for_exceptions(self, publisher_1: Runner, publisher_2: Runner, runner: Runner,
                              consumer_1: Runner, consumer_2: Runner) -> None:
        publisher_1.check_for_exceptions()
        publisher_2.check_for_exceptions()
        runner.check_for_exceptions()
        try:
            consumer_1.check_for_exceptions()
        except Exception as ex:
            self._consumer_1_error = ex
        try:
            consumer_2.check_for_exceptions()
        except Exception as ex:
            self._consumer_2_error = ex
