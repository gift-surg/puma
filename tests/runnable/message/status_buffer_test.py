import time
from dataclasses import dataclass
from typing import Optional, no_type_check
from unittest import TestCase

from puma.helpers.string import safe_str
from puma.runnable.message import StartedStatusMessage, StatusBuffer, StatusMessage
from puma.timeouts import TIMEOUT_NO_WAIT
from tests.buffer.test_support.test_inline_buffer import TestInlineBuffer
from tests.test_logging_helpers import assert_no_warnings_or_errors_logged

TIMEOUT = 0.5
TIME_TOLERANCE = 0.3


# This module tests StatusBuffer, in a single-threaded environment. Although this is not the intended use of the buffer, it can still detect a lot of issues, and
# they are much easier to investigate in single-threaded code.
# The buffer is tested in its intended cross-thread and cross-process usage in another test case.


class StatusMessageA(StatusMessage):
    pass


@dataclass(frozen=True)
class StatusMessageB(StatusMessage):
    v: int


class StatusMessageC(StatusMessage):
    pass


class StatusBufferTest(TestCase):
    @assert_no_warnings_or_errors_logged
    def test_block_until_running_normal_case(self) -> None:
        with TestInlineBuffer[StatusMessage](10, "test buffer") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.publish() as status_buffer_publisher, status_buffer.subscribe() as status_buffer_subscription:
                self.assertFalse(status_buffer_subscription.block_until_running(TIMEOUT_NO_WAIT))
                status_buffer_publisher.publish_status(StartedStatusMessage())
                self.assertTrue(status_buffer_subscription.block_until_running(TIMEOUT_NO_WAIT))
                status_buffer_publisher.publish_complete(None)

    @assert_no_warnings_or_errors_logged
    def test_block_until_running_timeout(self) -> None:
        with TestInlineBuffer[StatusMessage](10, "test buffer") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.subscribe() as status_buffer_subscription:
                self.assertFalse(status_buffer_subscription.block_until_running(0.01))

    @assert_no_warnings_or_errors_logged
    def test_block_until_running_ends_quickly_without_error(self) -> None:
        with TestInlineBuffer[StatusMessage](10, "test buffer") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.publish() as status_buffer_publisher, status_buffer.subscribe() as status_buffer_subscription:
                self.assertFalse(status_buffer_subscription.block_until_running(TIMEOUT_NO_WAIT))
                status_buffer_publisher.publish_status(StartedStatusMessage())
                status_buffer_publisher.publish_complete(None)
                self.assertTrue(status_buffer_subscription.block_until_running(TIMEOUT_NO_WAIT))

    @assert_no_warnings_or_errors_logged
    def test_block_until_running_ends_quickly_with_error(self) -> None:
        with TestInlineBuffer[StatusMessage](10, "test buffer") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.publish() as status_buffer_publisher, status_buffer.subscribe() as status_buffer_subscription:
                self.assertFalse(status_buffer_subscription.block_until_running(TIMEOUT_NO_WAIT))
                status_buffer_publisher.publish_status(StartedStatusMessage())
                ex = RuntimeError("Test error")
                status_buffer_publisher.publish_complete(ex)
                with self.assertRaisesRegex(RuntimeError, "Test error"):
                    status_buffer_subscription.block_until_running(TIMEOUT_NO_WAIT)

    @assert_no_warnings_or_errors_logged
    def test_check_for_exceptions_when_no_error(self) -> None:
        with TestInlineBuffer[StatusMessage](10, "test buffer") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.publish() as status_buffer_publisher, status_buffer.subscribe() as status_buffer_subscription:
                status_buffer_publisher.publish_complete(None)
                status_buffer_subscription.check_for_exceptions()

    @assert_no_warnings_or_errors_logged
    def test_check_for_exceptions_when_error(self) -> None:
        with TestInlineBuffer[StatusMessage](10, "test buffer") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.publish() as status_buffer_publisher, status_buffer.subscribe() as status_buffer_subscription:
                ex = RuntimeError("Test error")
                status_buffer_publisher.publish_complete(ex)
                with self.assertRaisesRegex(RuntimeError, "Test error"):
                    status_buffer_subscription.check_for_exceptions()

    @assert_no_warnings_or_errors_logged
    def test_check_for_exceptions_only_raises_once(self) -> None:
        with TestInlineBuffer[StatusMessage](10, "test buffer") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.publish() as status_buffer_publisher, status_buffer.subscribe() as status_buffer_subscription:
                ex = RuntimeError("Test error")
                status_buffer_publisher.publish_complete(ex)
                with self.assertRaisesRegex(RuntimeError, "Test error"):
                    status_buffer_subscription.check_for_exceptions()
                status_buffer_subscription.check_for_exceptions()  # no error

    @assert_no_warnings_or_errors_logged
    def test_error_trapped_by_context_management(self) -> None:
        with TestInlineBuffer[StatusMessage](10, "test buffer") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.publish() as status_buffer_publisher:
                status_buffer_subscription = status_buffer.subscribe().__enter__()
                ex = RuntimeError("Test error")
                status_buffer_publisher.publish_complete(ex)
                with self.assertRaisesRegex(RuntimeError, "Test error"):
                    status_buffer_subscription.__exit__(None, None, None)

    @assert_no_warnings_or_errors_logged
    def test_get_latest_status_message_when_found_no_wait(self) -> None:
        with TestInlineBuffer[StatusMessage](10, "test buffer") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.publish() as status_buffer_publisher, status_buffer.subscribe() as status_buffer_subscription:
                status_buffer_publisher.publish_status(StatusMessageA())
                status_buffer_publisher.publish_status(StatusMessageB(7))
                status_buffer_publisher.publish_status(StatusMessageC())
                t1 = time.perf_counter()
                message_b = status_buffer_subscription.get_latest_status_message(StatusMessageB)
                t2 = time.perf_counter()
                self.assert_is_message_b(message_b, 7)
                self.assertTrue(t2 - t1 <= TIME_TOLERANCE, f"Wait should have returned immediately, took {t2 - t1}")
                message_c = status_buffer_subscription.get_latest_status_message(StatusMessageC)
                self.assertTrue(isinstance(message_c, StatusMessageC))

    @assert_no_warnings_or_errors_logged
    def test_get_latest_status_message_forgets_value_once_found(self) -> None:
        with TestInlineBuffer[StatusMessage](10, "test buffer") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.publish() as status_buffer_publisher, status_buffer.subscribe() as status_buffer_subscription:
                status_buffer_publisher.publish_status(StatusMessageA())
                status_buffer_publisher.publish_status(StatusMessageB(7))
                status_buffer_publisher.publish_status(StatusMessageC())
                message_b = status_buffer_subscription.get_latest_status_message(StatusMessageB)
                self.assert_is_message_b(message_b, 7)
                message_b = status_buffer_subscription.get_latest_status_message(StatusMessageB)
                self.assertIsNone(message_b)
                message_c = status_buffer_subscription.get_latest_status_message(StatusMessageC)
                self.assertTrue(isinstance(message_c, StatusMessageC))

    @assert_no_warnings_or_errors_logged
    def test_get_latest_status_message_remembers_latest_value(self) -> None:
        with TestInlineBuffer[StatusMessage](10, "test buffer") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.publish() as status_buffer_publisher, status_buffer.subscribe() as status_buffer_subscription:
                status_buffer_publisher.publish_status(StatusMessageA())
                status_buffer_publisher.publish_status(StatusMessageB(7))
                status_buffer_publisher.publish_status(StatusMessageC())
                status_buffer_publisher.publish_status(StatusMessageB(8))  # previous message of the same type, containing value 7, is overwritten and lost
                message_b = status_buffer_subscription.get_latest_status_message(StatusMessageB)
                self.assert_is_message_b(message_b, 8)
                message_b = status_buffer_subscription.get_latest_status_message(StatusMessageB)
                self.assertIsNone(message_b)

    @assert_no_warnings_or_errors_logged
    def test_wait_for_status_message_when_not_found_no_wait(self) -> None:
        with TestInlineBuffer[StatusMessage](10, "test buffer") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.publish() as status_buffer_publisher, status_buffer.subscribe() as status_buffer_subscription:
                status_buffer_publisher.publish_status(StatusMessageA())
                status_buffer_publisher.publish_status(StatusMessageC())
                t1 = time.perf_counter()
                with self.assertRaisesRegex(TimeoutError, "Unable to retrieve status message .*StatusMessageB.* within timeout"):
                    status_buffer_subscription.wait_for_status_message(StatusMessageB, timeout=TIMEOUT_NO_WAIT)
                t2 = time.perf_counter()
                self.assertTrue(t2 - t1 <= TIME_TOLERANCE, f"Wait should have returned immediately, took {t2 - t1}")

    @assert_no_warnings_or_errors_logged
    def test_wait_for_status_message_when_empty(self) -> None:
        with TestInlineBuffer[StatusMessage](10, "test buffer") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.subscribe() as status_buffer_subscription:
                with self.assertRaisesRegex(TimeoutError, "Unable to retrieve status message .*StatusMessageB.* within timeout"):
                    status_buffer_subscription.wait_for_status_message(StatusMessageB, timeout=TIMEOUT_NO_WAIT)

    @assert_no_warnings_or_errors_logged
    def test_wait_for_status_message_timeout_when_found(self) -> None:
        with TestInlineBuffer[StatusMessage](10, "test buffer") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.publish() as status_buffer_publisher, status_buffer.subscribe() as status_buffer_subscription:
                status_buffer_publisher.publish_status(StatusMessageA())
                status_buffer_publisher.publish_status(StatusMessageC())
                t1 = time.perf_counter()
                message_a = status_buffer_subscription.wait_for_status_message(StatusMessageA, timeout=TIMEOUT)
                t2 = time.perf_counter()
                self.assertTrue(isinstance(message_a, StatusMessageA))
                self.assertTrue(t2 - t1 <= TIME_TOLERANCE, f"Wait should have returned immediately, took {t2 - t1}")

    @assert_no_warnings_or_errors_logged
    def test_wait_for_status_message_timeout_when_not_found(self) -> None:
        with TestInlineBuffer[StatusMessage](10, "test buffer") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.publish() as status_buffer_publisher, status_buffer.subscribe() as status_buffer_subscription:
                status_buffer_publisher.publish_status(StatusMessageA())
                status_buffer_publisher.publish_status(StatusMessageC())
                t1 = time.perf_counter()
                with self.assertRaisesRegex(TimeoutError, "Unable to retrieve status message .*StatusMessageB.* within timeout"):
                    status_buffer_subscription.wait_for_status_message(StatusMessageB, timeout=TIMEOUT)
                t2 = time.perf_counter()
                self.assertTrue(TIMEOUT - TIME_TOLERANCE <= t2 - t1 <= TIMEOUT + TIME_TOLERANCE, f"Wait should have timed out after {TIMEOUT}, took {t2 - t1}")

    @assert_no_warnings_or_errors_logged
    def test_wait_for_status_message_returns_immediately_once_finished_sent(self) -> None:
        # If publish_complete is sent while we are waiting for some status, the wait should fail immediately
        with TestInlineBuffer[StatusMessage](10, "test buffer") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.publish() as status_buffer_publisher, status_buffer.subscribe() as status_buffer_subscription:
                status_buffer_publisher.publish_status(StatusMessageA())
                status_buffer_publisher.publish_complete(error=None)
                t1 = time.perf_counter()
                with self.assertRaisesRegex(TimeoutError, "Unable to retrieve status message .*StatusMessageB.* as the Runnable has ended without publishing a matching message"):
                    status_buffer_subscription.wait_for_status_message(StatusMessageB, timeout=TIMEOUT)
                t2 = time.perf_counter()
                self.assertTrue(t2 - t1 <= TIME_TOLERANCE, f"Wait should have returned immediately because Complete, took {t2 - t1}")

    @assert_no_warnings_or_errors_logged
    def test_wait_for_status_message_returns_message_if_finished_sent(self) -> None:
        # If a given message that we assk for is already present then it should be returned, even if complete has later been published
        with TestInlineBuffer[StatusMessage](10, "test buffer") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.publish() as status_buffer_publisher, status_buffer.subscribe() as status_buffer_subscription:
                status_buffer_publisher.publish_status(StatusMessageB(7))
                status_buffer_publisher.publish_complete(error=None)
                t1 = time.perf_counter()
                message_b = status_buffer_subscription.wait_for_status_message(StatusMessageB, timeout=TIMEOUT)
                t2 = time.perf_counter()
                self.assert_is_message_b(message_b, 7)
                self.assertTrue(t2 - t1 <= TIME_TOLERANCE, f"Wait should have returned immediately because Complete, took {t2 - t1}")

    @assert_no_warnings_or_errors_logged
    def test_start_message_can_be_waited_for(self) -> None:
        # StartedStatusMessage is a special case, because StatusBuffer has the block_until_running method. Test that StartedStatusMessage can be treated like any other status.
        with TestInlineBuffer[StatusMessage](10, "test buffer") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.publish() as status_buffer_publisher, status_buffer.subscribe() as status_buffer_subscription:
                status_buffer_publisher.publish_status(StartedStatusMessage())

                t1 = time.perf_counter()
                message = status_buffer_subscription.wait_for_status_message(StartedStatusMessage, timeout=TIMEOUT)
                t2 = time.perf_counter()
                self.assertTrue(isinstance(message, StartedStatusMessage))
                self.assertTrue(t2 - t1 <= TIME_TOLERANCE, f"Wait should have returned immediately, took {t2 - t1}")

    # noinspection PyTypeChecker
    @no_type_check
    def test_illegal_params(self) -> None:
        with self.assertRaises(ValueError):
            StatusBuffer(None)
        with TestInlineBuffer[StatusMessage](10, "test buffer") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.publish() as status_buffer_publisher, status_buffer.subscribe() as status_buffer_subscription:
                with self.assertRaises(ValueError):
                    status_buffer_publisher.publish_status(None)
                with self.assertRaises(ValueError):
                    status_buffer_publisher.publish_complete(123)
                with self.assertRaises(ValueError):
                    status_buffer_subscription.block_until_running("string")
                with self.assertRaises(ValueError):
                    status_buffer_subscription.block_until_running(None)
                with self.assertRaises(ValueError):
                    status_buffer_subscription.block_until_running(0.0)
                with self.assertRaises(ValueError):
                    status_buffer_subscription.block_until_running(-1.0)
                with self.assertRaises(ValueError):
                    status_buffer_subscription.wait_for_status_message(None, TIMEOUT_NO_WAIT)
                with self.assertRaises(ValueError):
                    status_buffer_subscription.wait_for_status_message(StartedStatusMessage, "string")
                with self.assertRaises(ValueError):
                    status_buffer_subscription.wait_for_status_message(StartedStatusMessage, None)
                with self.assertRaises(ValueError):
                    status_buffer_subscription.wait_for_status_message(StartedStatusMessage, 0.0)
                with self.assertRaises(ValueError):
                    status_buffer_subscription.wait_for_status_message(StartedStatusMessage, -1.0)
                with self.assertRaises(ValueError):
                    status_buffer_subscription.get_latest_status_message(None)

    def assert_is_message_b(self, message: Optional[StatusMessage], expected_value: int) -> None:
        self.assertIsNotNone(message)
        if isinstance(message, StatusMessageB):
            self.assertEqual(expected_value, message.v)
        else:
            self.fail(f"Incorrect type of message received: {safe_str(message)}")
