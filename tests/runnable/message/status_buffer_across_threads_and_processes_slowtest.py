import logging
import time
from dataclasses import dataclass
from typing import Optional
from unittest import TestCase

from puma.helpers.string import safe_str
from puma.helpers.testing.logging.decorator import assert_no_warnings_or_errors_logged
from puma.helpers.testing.parameterized import parameterized
from puma.primitives import EventType
from puma.runnable.message import StartedStatusMessage, StatusBuffer, StatusMessage
from puma.timeouts import TIMEOUT_NO_WAIT
from tests.environment.parameterisation import EnvironmentTestParameters, environments

logger = logging.getLogger(__name__)

BUFFER_SIZE = 10
DELAY = 0.5
TIME_TOLERANCE = 0.3


class StatusMessageA(StatusMessage):
    # Never sent
    pass


@dataclass(frozen=True)
class StatusMessageB(StatusMessage):
    # Status sent by Remote
    v: int


class _Remote:
    def __init__(self, param: EnvironmentTestParameters, status_buffer: StatusBuffer, started_event: EventType,
                 delay: float,
                 complete_delay: Optional[float] = None,
                 error: bool = False,
                 send_complete: bool = True) -> None:
        env = param.environment
        self._status_buffer = status_buffer
        self._started_event = started_event
        self._delay = delay
        self._complete_delay = complete_delay if complete_delay is not None else delay
        self._error = error
        self._send_complete = send_complete
        self._active = env.create_thread_or_process(name="remote", target=self._run)

    def start(self) -> None:
        self._active.start()

    def join(self, timeout: float) -> None:
        self._active.join(timeout)

    def is_alive(self) -> bool:
        return self._active.is_alive()

    def _run(self) -> None:
        with self._status_buffer.publish() as status_buffer_publisher:
            try:
                self._started_event.set()
                time.sleep(self._delay)
                if self._error:
                    raise RuntimeError("Test error")
                status_buffer_publisher.publish_status(StartedStatusMessage())
                status_buffer_publisher.publish_status(StatusMessageB(7))
            except Exception as ex:
                if not self._error:
                    logger.error("Error in Remote: %s", safe_str(ex), exc_info=True)
                if self._send_complete:
                    # sleep, so we can be sure that the receiver is not waiting for Complete
                    time.sleep(self._complete_delay)
                    status_buffer_publisher.publish_complete(ex)
            else:
                if self._send_complete:
                    # sleep, so we can be sure that the receiver is not waiting for Complete
                    time.sleep(self._complete_delay)
                    status_buffer_publisher.publish_complete(None)


class StatusBufferAcrossThreadsAndProcessesSlowTest(TestCase):
    @parameterized(environments)
    @assert_no_warnings_or_errors_logged
    def test_normal_case(self, param: EnvironmentTestParameters) -> None:
        env = param.environment
        started_event = env.create_event()
        with env.create_buffer(StatusMessage, BUFFER_SIZE, "status") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.subscribe() as status_buffer_subscription:
                remote = _Remote(param, status_buffer, started_event, delay=DELAY)
                remote.start()
                self.assertTrue(started_event.wait(5.0))
                try:
                    t1 = time.perf_counter()
                    self.assertTrue(status_buffer_subscription.block_until_running(DELAY * 5))
                    t2 = time.perf_counter()
                    self.assertGreater(t2 - t1, DELAY - TIME_TOLERANCE, f"block_until_running should have blocked for at least {DELAY}, took {t2 - t1}")
                    self.assertLess(t2 - t1, DELAY + TIME_TOLERANCE, f"block_until_running should have blocked for only {DELAY}, took {t2 - t1}")

                    status_buffer_subscription.check_for_exceptions()  # no errors
                finally:
                    remote.join(10.0)
                    self.assertFalse(remote.is_alive())

    @parameterized(environments)
    @assert_no_warnings_or_errors_logged
    def test_when_timeout(self, param: EnvironmentTestParameters) -> None:
        env = param.environment
        started_event = env.create_event()
        with env.create_buffer(StatusMessage, BUFFER_SIZE, "status") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.subscribe() as status_buffer_subscription:
                _Remote(param, status_buffer, started_event, 1.0)  # created but never started
                self.assertFalse(status_buffer_subscription.block_until_running(TIMEOUT_NO_WAIT))
                status_buffer_subscription.check_for_exceptions()  # no errors

    @parameterized(environments)
    @assert_no_warnings_or_errors_logged
    def test_when_ends_quickly_without_error(self, param: EnvironmentTestParameters) -> None:
        env = param.environment
        started_event = env.create_event()
        with env.create_buffer(StatusMessage, BUFFER_SIZE, "status") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.subscribe() as status_buffer_subscription:
                remote = _Remote(param, status_buffer, started_event, delay=0.0)
                remote.start()
                self.assertTrue(started_event.wait(5.0))
                try:
                    t1 = time.perf_counter()
                    self.assertTrue(status_buffer_subscription.block_until_running(DELAY))
                    t2 = time.perf_counter()
                    self.assertLess(t2 - t1, TIME_TOLERANCE, f"block_until_running should have returned immediately, took {t2 - t1}")

                    status_buffer_subscription.check_for_exceptions()  # no errors
                finally:
                    remote.join(10.0)
                    self.assertFalse(remote.is_alive())

    @parameterized(environments)
    @assert_no_warnings_or_errors_logged
    def test_when_ends_quickly_with_error_error_reported_by_block_until_running(self, param: EnvironmentTestParameters) -> None:
        # The runner raises an error; if the first thing we then do is block_until_running, check this re-raises the error in the client
        env = param.environment
        started_event = env.create_event()
        with env.create_buffer(StatusMessage, BUFFER_SIZE, "status") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.subscribe() as status_buffer_subscription:
                remote = _Remote(param, status_buffer, started_event, delay=0.0, error=True)
                remote.start()
                self.assertTrue(started_event.wait(5.0))
                try:
                    t1 = time.perf_counter()
                    with self.assertRaisesRegex(RuntimeError, "Test error"):
                        status_buffer_subscription.block_until_running(DELAY)
                    t2 = time.perf_counter()
                    self.assertLess(t2 - t1, TIME_TOLERANCE, f"block_until_running should have returned immediately, took {t2 - t1}")

                    status_buffer_subscription.check_for_exceptions()  # no error, error must only be raised once
                finally:
                    remote.join(10.0)
                    self.assertFalse(remote.is_alive())

    @parameterized(environments)
    @assert_no_warnings_or_errors_logged
    def test_when_ends_quickly_with_error_error_reported_by_check_for_exceptions(self, param: EnvironmentTestParameters) -> None:
        # The runner raises an error; if the first thing we then do is check_for_exceptions, check this re-raises the error in the client
        env = param.environment
        started_event = env.create_event()
        with env.create_buffer(StatusMessage, BUFFER_SIZE, "status") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.subscribe() as status_buffer_subscription:
                remote = _Remote(param, status_buffer, started_event, delay=0.0, error=True)
                remote.start()
                self.assertTrue(started_event.wait(5.0))
                try:
                    time.sleep(0.5)
                    with self.assertRaisesRegex(RuntimeError, "Test error"):
                        status_buffer_subscription.check_for_exceptions()
                finally:
                    remote.join(10.0)
                    self.assertFalse(remote.is_alive())

    @parameterized(environments)
    @assert_no_warnings_or_errors_logged
    def test_when_ends_quickly_with_error_error_reported_by_status_buffer_context_management(self, param: EnvironmentTestParameters) -> None:
        # The runner raises an error; if we don't poll for errors, make sure they are re-raised in the client when the buffer goes out of context management
        env = param.environment
        started_event = env.create_event()
        with env.create_buffer(StatusMessage, BUFFER_SIZE, "status") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            status_buffer_subscription = status_buffer.subscribe().__enter__()
            remote = _Remote(param, status_buffer, started_event, delay=0.0, error=True)
            remote.start()
            self.assertTrue(started_event.wait(5.0))
            remote.join(10.0)
            self.assertFalse(remote.is_alive())
            with self.assertRaisesRegex(RuntimeError, "Test error"):
                status_buffer_subscription.__exit__(None, None, None)

    @parameterized(environments)
    @assert_no_warnings_or_errors_logged
    def test_wait_for_status_message_when_found(self, param: EnvironmentTestParameters) -> None:
        env = param.environment
        started_event = env.create_event()
        with env.create_buffer(StatusMessage, BUFFER_SIZE, "status") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.subscribe() as status_buffer_subscription:
                remote = _Remote(param, status_buffer, started_event, delay=DELAY)
                remote.start()
                self.assertTrue(started_event.wait(5.0))
                try:
                    t1 = time.perf_counter()
                    message = status_buffer_subscription.wait_for_status_message(StatusMessageB, DELAY * 5)
                    t2 = time.perf_counter()
                    self.assert_is_message_b(message, 7)
                    self.assertGreater(t2 - t1, DELAY - TIME_TOLERANCE, f"wait_for_status_message should have blocked for at least {DELAY}, took {t2 - t1}")
                    self.assertLess(t2 - t1, DELAY + TIME_TOLERANCE, f"wait_for_status_message should have blocked for only {DELAY}, took {t2 - t1}")

                    status_buffer_subscription.check_for_exceptions()  # no errors
                finally:
                    remote.join(10.0)
                    self.assertFalse(remote.is_alive())

    @parameterized(environments)
    @assert_no_warnings_or_errors_logged
    def test_wait_for_status_message_when_not_found_but_finished(self, param: EnvironmentTestParameters) -> None:
        env = param.environment
        started_event = env.create_event()
        with env.create_buffer(StatusMessage, BUFFER_SIZE, "status") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.subscribe() as status_buffer_subscription:
                remote = _Remote(param, status_buffer, started_event, delay=DELAY, complete_delay=0.0)
                remote.start()
                self.assertTrue(started_event.wait(5.0))
                try:
                    t1 = time.perf_counter()
                    with self.assertRaisesRegex(TimeoutError,
                                                "Unable to retrieve status message .*StatusMessageA.* as the Runnable has ended without publishing a matching message"):
                        status_buffer_subscription.wait_for_status_message(StatusMessageA, DELAY * 5)
                    t2 = time.perf_counter()
                    self.assertGreater(t2 - t1, DELAY - TIME_TOLERANCE, f"wait_for_status_message should have blocked for at least {DELAY}, took {t2 - t1}")
                    self.assertLess(t2 - t1, DELAY + TIME_TOLERANCE, f"wait_for_status_message should have blocked for only {DELAY}, took {t2 - t1}")

                    status_buffer_subscription.check_for_exceptions()  # no errors
                finally:
                    remote.join(10.0)
                    self.assertFalse(remote.is_alive())

    @parameterized(environments)
    @assert_no_warnings_or_errors_logged
    def test_wait_for_status_message_when_not_found_and_not_finished(self, param: EnvironmentTestParameters) -> None:
        env = param.environment
        started_event = env.create_event()
        with env.create_buffer(StatusMessage, BUFFER_SIZE, "status") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.subscribe() as status_buffer_subscription:
                remote = _Remote(param, status_buffer, started_event, delay=0.0, send_complete=False)
                remote.start()
                self.assertTrue(started_event.wait(5.0))
                try:
                    t1 = time.perf_counter()
                    with self.assertRaisesRegex(TimeoutError, "Unable to retrieve status message .*StatusMessageA.* within timeout"):
                        status_buffer_subscription.wait_for_status_message(StatusMessageA, DELAY)
                    t2 = time.perf_counter()
                    self.assertGreater(t2 - t1, DELAY - TIME_TOLERANCE, f"wait_for_status_message should have blocked for at least {DELAY}, took {t2 - t1}")
                    self.assertLess(t2 - t1, DELAY + TIME_TOLERANCE, f"wait_for_status_message should have blocked for only {DELAY}, took {t2 - t1}")

                    status_buffer_subscription.check_for_exceptions()  # no errors
                finally:
                    remote.join(10.0)
                    self.assertFalse(remote.is_alive())

    @parameterized(environments)
    @assert_no_warnings_or_errors_logged
    def test_wait_for_status_message_can_wait_for_start_message(self, param: EnvironmentTestParameters) -> None:
        env = param.environment
        started_event = env.create_event()
        with env.create_buffer(StatusMessage, BUFFER_SIZE, "status") as wrapped_buffer:
            status_buffer = StatusBuffer(wrapped_buffer)
            with status_buffer.subscribe() as status_buffer_subscription:
                remote = _Remote(param, status_buffer, started_event, delay=DELAY)
                remote.start()
                self.assertTrue(started_event.wait(5.0))
                try:
                    t1 = time.perf_counter()
                    message = status_buffer_subscription.wait_for_status_message(StartedStatusMessage, DELAY * 5)
                    t2 = time.perf_counter()
                    self.assertIsNotNone(message)
                    self.assertTrue(isinstance(message, StartedStatusMessage))
                    self.assertGreater(t2 - t1, DELAY - TIME_TOLERANCE, f"wait_for_status_message should have blocked for at least {DELAY}, took {t2 - t1}")
                    self.assertLess(t2 - t1, DELAY + TIME_TOLERANCE, f"wait_for_status_message should have blocked for only {DELAY}, took {t2 - t1}")

                    status_buffer_subscription.check_for_exceptions()  # no errors
                finally:
                    remote.join(10.0)
                    self.assertFalse(remote.is_alive())

    def assert_is_message_b(self, message: Optional[StatusMessage], expected_value: int) -> None:
        self.assertIsNotNone(message)
        if isinstance(message, StatusMessageB):
            self.assertEqual(expected_value, message.v)
        else:
            self.fail(f"Incorrect type of message received: {safe_str(message)}")
