import sys
import time
import traceback
from typing import Iterable, Optional
from unittest import TestCase

from puma.attribute import unmanaged
from puma.buffer import MultiProcessBuffer, Observable, Publishable, Subscriber
from puma.runnable import SingleBufferServicingRunnable
from puma.runnable.runner import ProcessRunner
from tests.runnable.test_support.forwarding_subscriber import ForwardingSubscriber
from tests.runnable.test_support.publishing_runnable import PublishingRunnable
from tests.runnable.test_support.testval import TestVal
from tests.runnable.test_support.validating_single_subscription_runnable import ValidatingSingleSubscriptionRunnable

BUFFER_SIZE = 10
COUNT = BUFFER_SIZE
PUSH_INTERVAL = 0.001
FAIL_TIMEOUT = 10.0
TEST_TIMEOUT = 30.0


# Test that errors move between threads and processes without losing the stack trace.


class _ForwardingRunnable(SingleBufferServicingRunnable, Subscriber[TestVal]):
    _forwarder: ForwardingSubscriber = unmanaged("_forwarder")

    def __init__(self, in_buffer: Observable[TestVal], out_buffer: Publishable[TestVal], name: str) -> None:
        super().__init__(in_buffer, self, [out_buffer], name)
        publisher = self._get_publisher_unwrapped(out_buffer)
        self._forwarder = ForwardingSubscriber(in_buffer.buffer_name(), publisher)

    def on_value(self, value: TestVal) -> None:
        self._forwarder.on_value(value)

    def on_complete(self, error: Optional[BaseException]) -> None:
        self._forwarder.on_complete(error)


class ErrorChainingAcrossProcessesSlowTest(TestCase):
    def test_ends_when_buffers_completed(self) -> None:
        # An error is raised in the publishing process. This should end the publishing process, and get passed through the chain of processes, ending them,
        # and should finally be caught as an error from the final ("Consumer") process.
        # The error will probably be logged, too, because the publishing process was not ended cleanly, but this is not tested here - logging is tested elsewhere.
        final_exception: Optional[Exception] = None
        with MultiProcessBuffer[TestVal](BUFFER_SIZE, "Publish buffer") as publish_buffer, \
                MultiProcessBuffer[TestVal](BUFFER_SIZE, "Intermediate buffer") as intermediate_buffer, \
                MultiProcessBuffer[TestVal](BUFFER_SIZE, "Subscriber buffer") as subscriber_buffer:
            with ProcessRunner(PublishingRunnable(COUNT, publish_buffer, delay=PUSH_INTERVAL, do_error=True), "Publisher") as publish_process, \
                    ProcessRunner(_ForwardingRunnable(publish_buffer, intermediate_buffer, "Forwarding runnable 1")) as forwarding_process_1, \
                    ProcessRunner(_ForwardingRunnable(intermediate_buffer, subscriber_buffer, "Forwarding runnable 2")) as forwarding_process_2, \
                    ProcessRunner(ValidatingSingleSubscriptionRunnable(subscriber_buffer, COUNT, should_receive_all=False, raise_received_completion_errors=True),
                                  "Consumer") as subscriber_process:
                test_fail_time = time.monotonic() + TEST_TIMEOUT

                forwarding_process_1.start()
                forwarding_process_2.start()
                subscriber_process.start()
                forwarding_process_1.wait_until_running(FAIL_TIMEOUT)
                forwarding_process_2.wait_until_running(FAIL_TIMEOUT)
                subscriber_process.wait_until_running(FAIL_TIMEOUT)
                publish_process.start()
                publish_process.wait_until_running(FAIL_TIMEOUT)

                while subscriber_process.is_alive():
                    # noinspection PyBroadException
                    try:
                        publish_process.check_for_exceptions()
                    except Exception:
                        self.fail("Unexpected exception from publish process")

                    # noinspection PyBroadException
                    try:
                        forwarding_process_1.check_for_exceptions()
                        forwarding_process_2.check_for_exceptions()
                    except Exception:
                        self.fail("Unexpected exception from forwarding process")

                    try:
                        subscriber_process.check_for_exceptions()
                    except Exception as ex:
                        final_exception = ex

                    time.sleep(0.5)
                    self.assertLess(time.monotonic(), test_fail_time, "Deadlock: Test did not end")

                # check for exception again, rather than having it thrown when context management ends, so we can check it
                try:
                    subscriber_process.check_for_exceptions()
                except Exception as ex:
                    final_exception = ex

        if final_exception:
            try:
                raise final_exception
            except RuntimeError:
                lines = traceback.format_exception(*sys.exc_info())
                tidied = []
                for line in lines:
                    tidied.extend([subline.strip() for subline in line.splitlines()])
                self._assert_contains_substring(f'publishing_runnable.py', tidied)
                self._assert_contains_substring('raise RuntimeError("Test Error")', tidied)
        else:
            self.fail("Failed to receive the expected error")

    def _assert_contains_substring(self, substring: str, lines: Iterable[str]) -> None:
        self.assertTrue(self._contains_substring(substring, lines), f"Failed to find substring '{substring}' in reported lines: {lines}")

    @staticmethod
    def _contains_substring(substring: str, lines: Iterable[str]) -> bool:
        for line in lines:
            if substring in line:
                return True
        return False
