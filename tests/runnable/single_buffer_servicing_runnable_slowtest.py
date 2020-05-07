import time
from typing import Optional
from unittest import TestCase

from puma.attribute import unmanaged
from puma.buffer import Observable, Publishable, Subscriber
from puma.helpers.string import safe_str
from puma.helpers.testing.logging.decorator import assert_no_warnings_or_errors_logged
from puma.helpers.testing.mixin import NotATestCase
from puma.helpers.testing.parameterized import parameterized
from puma.runnable import SingleBufferServicingRunnable
from puma.runnable.runner import Runner
from tests.environment.parameterisation import EnvironmentTestParameters, environments
from tests.runnable.test_support.forwarding_subscriber import ForwardingSubscriber
from tests.runnable.test_support.publishing_runnable import PublishingRunnable
from tests.runnable.test_support.testval import TestVal
from tests.runnable.test_support.validating_single_subscription_runnable import ValidatingSingleSubscriptionRunnable

BUFFER_SIZE = 10
COUNT = BUFFER_SIZE * 10
PUSH_INTERVAL = 0.02
PUSH_INTERVAL_2 = 0.05
FAIL_TIMEOUT = 10.0
TEST_TIMEOUT = 30.0

STOP_DELAY = COUNT * min(PUSH_INTERVAL, PUSH_INTERVAL_2) / 2


# Test SingleBufferServicingRunnable across threads and processes. Most of the testing of SingleBufferServicingRunnable is done in a single thread, see
# single_buffer_servicing_runnable_test.py. This 'slow' test ensures that it really works across threads and processes.


class TestSingleBufferServicingRunnable(SingleBufferServicingRunnable, Subscriber[TestVal], NotATestCase):
    # Test class for SingleBufferServicingRunnable. Copies values received on its input buffer to its output buffer.
    _forwarder: ForwardingSubscriber = unmanaged("_forwarder")

    def __init__(self, in_buffer: Observable[TestVal], out_buffer: Publishable[TestVal], cause_error_at_count: int = -1) -> None:
        super().__init__(in_buffer, self, [out_buffer], "Test runnable")
        publisher = self._get_publisher_unwrapped(out_buffer)
        self._forwarder = ForwardingSubscriber(in_buffer.buffer_name(), publisher, cause_error_at_count=cause_error_at_count)

    def on_value(self, value: TestVal) -> None:
        self._forwarder.on_value(value)

    def on_complete(self, error: Optional[BaseException]) -> None:
        self._forwarder.on_complete(error)


class SingleBufferServicingRunnableSlowTest(TestCase):
    def setUp(self) -> None:
        self._consumer_error: Optional[Exception] = None

    @parameterized(environments)
    @assert_no_warnings_or_errors_logged
    def test_ends_when_buffers_completed(self, param: EnvironmentTestParameters) -> None:
        env = param.environment
        with env.create_buffer(TestVal, BUFFER_SIZE, "Publish buffer") as publish_buffer, \
                env.create_buffer(TestVal, BUFFER_SIZE, "Subscriber buffer") as subscriber_buffer:
            runnable = TestSingleBufferServicingRunnable(publish_buffer, subscriber_buffer)
            with env.create_runner(PublishingRunnable(COUNT, publish_buffer, delay=PUSH_INTERVAL), "Publisher") as publisher, \
                    env.create_runner(ValidatingSingleSubscriptionRunnable(subscriber_buffer, COUNT), "Consumer") as consumer, \
                    env.create_runner(runnable) as runner:
                test_fail_time = time.monotonic() + TEST_TIMEOUT

                runner.start()
                consumer.start()
                runner.wait_until_running(FAIL_TIMEOUT)
                consumer.wait_until_running(FAIL_TIMEOUT)
                publisher.start()
                publisher.wait_until_running(FAIL_TIMEOUT)

                while consumer.is_alive():
                    publisher.check_for_exceptions()
                    consumer.check_for_exceptions()
                    runner.check_for_exceptions()
                    time.sleep(0.5)
                    self.assertLess(time.monotonic(), test_fail_time, "Deadlock: Test did not end")

    @parameterized(environments)
    @assert_no_warnings_or_errors_logged
    def test_ends_when_stopped(self, param: EnvironmentTestParameters) -> None:
        env = param.environment
        with env.create_buffer(TestVal, BUFFER_SIZE, "Publish buffer") as publish_buffer, \
                env.create_buffer(TestVal, BUFFER_SIZE, "Subscriber buffer") as subscriber_buffer:
            runnable = TestSingleBufferServicingRunnable(publish_buffer, subscriber_buffer)
            with env.create_runner(PublishingRunnable(COUNT, publish_buffer, delay=PUSH_INTERVAL, error_if_full=False, send_complete=False), "Publisher") as publisher, \
                    env.create_runner(ValidatingSingleSubscriptionRunnable(subscriber_buffer, COUNT, should_receive_all=False), "Consumer") as consumer, \
                    env.create_runner(runnable) as runner:
                test_fail_time = time.monotonic() + TEST_TIMEOUT

                runner.start()
                consumer.start()
                runner.wait_until_running(FAIL_TIMEOUT)
                consumer.wait_until_running(FAIL_TIMEOUT)

                publisher.start()
                publisher.wait_until_running(FAIL_TIMEOUT)

                stop_time = time.monotonic() + STOP_DELAY
                while consumer.is_alive():
                    publisher.check_for_exceptions()
                    consumer.check_for_exceptions()
                    runner.check_for_exceptions()
                    time.sleep(0.5)
                    if time.monotonic() > stop_time:
                        runner.stop()
                    if time.monotonic() > test_fail_time:
                        self.fail("Deadlock: Test did not end")

    @parameterized(environments)
    def test_when_error(self, param: EnvironmentTestParameters) -> None:
        env = param.environment
        with env.create_buffer(TestVal, BUFFER_SIZE, "Publish buffer") as publish_buffer, \
                env.create_buffer(TestVal, BUFFER_SIZE, "Subscriber buffer") as subscriber_buffer:
            runnable = TestSingleBufferServicingRunnable(publish_buffer, subscriber_buffer, cause_error_at_count=COUNT // 2)
            with env.create_runner(PublishingRunnable(COUNT, publish_buffer, delay=PUSH_INTERVAL, error_if_full=False, send_complete=False), "Publisher 1") as publisher, \
                    env.create_runner(ValidatingSingleSubscriptionRunnable(subscriber_buffer, COUNT, should_receive_all=False, raise_received_completion_errors=True),
                                      "Consumer 1") as consumer, \
                    env.create_runner(runnable) as runner:
                test_fail_time = time.monotonic() + TEST_TIMEOUT

                runner.start()
                consumer.start()
                runner.wait_until_running(FAIL_TIMEOUT)
                consumer.wait_until_running(FAIL_TIMEOUT)

                publisher.start()
                publisher.wait_until_running(FAIL_TIMEOUT)

                while consumer.is_alive():
                    self._check_for_exceptions(publisher, runner, consumer)
                    time.sleep(0.5)
                    self.assertLess(time.monotonic(), test_fail_time, "Deadlock: Test did not end")

                # check for errors again, because we may have stopped before testing; normally this would get raised when the
                # runner exits context management, but here we are trying to catch it to check it
                self._check_for_exceptions(publisher, runner, consumer)

                self.assertIsNotNone(self._consumer_error)
                self.assertEqual("RuntimeError('Test Error!')", safe_str(self._consumer_error))

    def _check_for_exceptions(self, publisher: Runner, runner: Runner, consumer: Runner) -> None:
        publisher.check_for_exceptions()
        runner.check_for_exceptions()
        try:
            consumer.check_for_exceptions()
        except Exception as ex:
            self._consumer_error = ex
