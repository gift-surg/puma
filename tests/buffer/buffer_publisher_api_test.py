import logging
import queue
import time
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Callable, Dict, no_type_check
from unittest import TestCase

from puma.buffer import Buffer, Observable
from puma.helpers.testing.logging.capture_logs import CaptureLogs
from puma.helpers.testing.logging.decorator import assert_no_warnings_or_errors_logged
from puma.helpers.testing.parameterized import parameterized
from puma.logging import LogLevel
from puma.primitives import AutoResetEvent
from puma.timeouts import TIMEOUT_INFINITE, TIMEOUT_NO_WAIT
from puma.unexpected_situation_action import UnexpectedSituationAction
from tests.buffer._parameterisation import BufferTestEnvironment, BufferTestParams, envs
from tests.buffer.test_support.buffer_api_test_support import TestSubscriber, fill_the_buffer, receive_all

logger = logging.getLogger(__name__)

BUFFER_SIZE = 10
TIMEOUT = 0.5
TIME_TOLERANCE = 0.3


class BufferPublisherApiTest(TestCase):

    def setUp(self) -> None:
        self._subscriber1: TestSubscriber = TestSubscriber()

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_push_timeout_no_wait_times_out_error(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            with buffer.publish() as publisher:
                fill_the_buffer(publisher, BUFFER_SIZE)
                env.publish_observe_delay()

                funcs: Dict[str, Callable[[], None]] = {
                    "*** publish_value": lambda: publisher.publish_value("excess", TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION),
                    "*** publish_complete": lambda: publisher.publish_complete(None, TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)}
                for desc, func in funcs.items():
                    logger.debug(desc)
                    t1 = time.perf_counter()
                    with self.assertRaises(queue.Full):
                        func()
                    t2 = time.perf_counter()
                    self.assertLess(t2 - t1, TIME_TOLERANCE, f"Should have failed immediately with option TIMEOUT_NO_WAIT, took {t2 - t1}")

    @parameterized(envs)
    def test_push_timeout_no_wait_times_out_warn(self, param: BufferTestParams) -> None:
        with assert_no_warnings_or_errors_logged(self):
            env = param._env
        with self._create_buffer(env) as buffer:
            with buffer.publish() as publisher:
                fill_the_buffer(publisher, BUFFER_SIZE)
                env.publish_observe_delay()

                funcs: Dict[str, Callable[[], None]] = {
                    "*** publish_value": lambda: publisher.publish_value("excess", TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.LOG_WARNING),
                    "*** publish_complete": lambda: publisher.publish_complete(None, TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.LOG_WARNING)}
                for desc, func in funcs.items():
                    logger.debug(desc)
                    with CaptureLogs(LogLevel.warn) as log_context:
                        t1 = time.perf_counter()
                        func()
                        t2 = time.perf_counter()
                        records = log_context.pop_captured_records()
                        self.assertTrue(records.containing_message("Buffer full").with_levels_in({LogLevel.warn}))
                    self.assertLess(t2 - t1, TIME_TOLERANCE, f"Should have failed immediately with option TIMEOUT_NO_WAIT, took {t2 - t1}")

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_push_timeout_no_wait_times_out_ignore(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            with buffer.publish() as publisher:
                fill_the_buffer(publisher, BUFFER_SIZE)
                env.publish_observe_delay()

                funcs: Dict[str, Callable[[], None]] = {
                    "*** publish_value": lambda: publisher.publish_value("excess", TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.IGNORE),
                    "*** publish_complete": lambda: publisher.publish_complete(None, TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.IGNORE)}
                for desc, func in funcs.items():
                    logger.debug(desc)
                    with CaptureLogs(LogLevel.debug) as log_context:
                        t1 = time.perf_counter()
                        func()
                        t2 = time.perf_counter()
                        self.assertLess(t2 - t1, TIME_TOLERANCE, f"Should have failed immediately with option TIMEOUT_NO_WAIT, took {t2 - t1}")
                        records = log_context.pop_captured_records()
                        self.assertFalse(records.containing_message("Buffer full").with_levels_in({LogLevel.warn, LogLevel.error, LogLevel.fatal}))

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_push_timeout_specified_times_out_error(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            with buffer.publish() as publisher:
                fill_the_buffer(publisher, BUFFER_SIZE)
                env.publish_observe_delay()

                funcs: Dict[str, Callable[[], None]] = {
                    "*** publish_value": lambda: publisher.publish_value("excess", TIMEOUT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION),
                    "*** publish_complete": lambda: publisher.publish_complete(None, TIMEOUT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)}
                for desc, func in funcs.items():
                    logger.debug(desc)
                    t1 = time.perf_counter()
                    with self.assertRaises(queue.Full):
                        func()
                    t2 = time.perf_counter()
                    self.assertTrue(TIMEOUT - TIME_TOLERANCE <= t2 - t1 <= TIMEOUT + TIME_TOLERANCE, f"Should have timed out after {TIMEOUT}, took {t2 - t1}")

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_push_timeout_specified_completes(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            with buffer.publish() as publisher:
                fill_the_buffer(publisher, BUFFER_SIZE)
                env.publish_observe_delay()

                funcs: Dict[str, Callable[[], None]] = {
                    "*** publish_value": lambda: publisher.publish_value("excess", TIMEOUT * 10, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION),
                    "*** publish_complete": lambda: publisher.publish_complete(None, TIMEOUT * 10, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)}
                for desc, func in funcs.items():
                    logger.debug(desc)
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(self._pop, buffer, TIMEOUT, 1)
                        func()
                        t1 = future.result()  # time when first value was popped by executor thread, making space
                        t2 = time.perf_counter()  # time now, when value pushed
                        self.assertLessEqual(0.0, t2 - t1, "Published before space was freed in the buffer")
                        self.assertGreaterEqual(TIME_TOLERANCE, t2 - t1, f"Should have pushed immediately after space was made, took {t2 - t1}")

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_push_timeout_infinite_completes(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            with buffer.publish() as publisher:
                fill_the_buffer(publisher, BUFFER_SIZE)
                env.publish_observe_delay()

                funcs: Dict[str, Callable[[], None]] = {
                    "*** publish_value": lambda: publisher.publish_value("excess", TIMEOUT_INFINITE, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION),
                    "*** publish_complete": lambda: publisher.publish_complete(None, TIMEOUT_INFINITE, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)}
                for desc, func in funcs.items():
                    logger.debug(desc)
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(self._pop, buffer, TIMEOUT, 1)
                        func()
                        t1 = future.result()  # time when first value was popped by executor thread, making space
                        t2 = time.perf_counter()  # time now, when value pushed
                        self.assertLessEqual(0.0, t2 - t1, "Published before space was freed in the buffer")
                        self.assertGreaterEqual(TIME_TOLERANCE, t2 - t1, f"Should have pushed immediately after space was made, took {t2 - t1}")

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_complete_with_error(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            event = AutoResetEvent()
            with buffer.publish() as publisher, buffer.subscribe(event) as subscription:
                publisher.publish_value("a")
                publisher.publish_complete(RuntimeError("Test Error"))
                env.publish_observe_delay()
                receive_all(subscription, event, self._subscriber1)
                self._subscriber1.assert_published_values(["a"], self)
                self._subscriber1.assert_completed(True, self)
                self._subscriber1.assert_error_values(["RuntimeError('Test Error')"], self)

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_push_after_complete(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            event = AutoResetEvent()
            with buffer.publish() as publisher, buffer.subscribe(event) as subscription:
                publisher.publish_value("a")
                publisher.publish_complete(None)
                with self.assertRaisesRegex(RuntimeError, "Trying to publish a value after publishing Complete"):
                    publisher.publish_value("b")
                env.publish_observe_delay()
                receive_all(subscription, event, self._subscriber1)
                self._subscriber1.assert_published_values(["a"], self)
                self._subscriber1.assert_completed(True, self)
                self._subscriber1.assert_error_values([], self)

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_multiple_completes(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            event = AutoResetEvent()
            with buffer.publish() as publisher, buffer.subscribe(event) as subscription:
                publisher.publish_value("a")
                publisher.publish_complete(None)
                with self.assertRaisesRegex(RuntimeError, "Trying to publish Complete more than once"):
                    publisher.publish_complete(None)
                env.publish_observe_delay()
                receive_all(subscription, event, self._subscriber1)
                self._subscriber1.assert_published_values(["a"], self)
                self._subscriber1.assert_completed(True, self)
                self._subscriber1.assert_error_values([], self)

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_multiple_complete_ok_if_first_failed(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            event = AutoResetEvent()
            with buffer.publish() as publisher, buffer.subscribe(event) as subscription:
                fill_the_buffer(publisher, BUFFER_SIZE)
                env.publish_observe_delay()
                with self.assertRaises(queue.Full):
                    publisher.publish_complete(None, timeout=TIMEOUT_NO_WAIT)  # fails because buffer if full
                subscription.call_events(self._subscriber1)  # make space

                publisher.publish_complete(None, timeout=TIMEOUT_NO_WAIT)  # no error
                env.publish_observe_delay()
                receive_all(subscription, event, self._subscriber1)
                self._subscriber1.assert_completed(True, self)
                self._subscriber1.assert_error_values([], self)

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_buffer_name(self, param: BufferTestParams) -> None:
        env = param._env
        with env.create_buffer(str, BUFFER_SIZE, "abuff_name") as buf:
            with buf.publish() as publisher:
                self.assertEqual("abuff_name", publisher.buffer_name())

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_value_can_be_falsy(self, param: BufferTestParams) -> None:
        env = param._env

        with self._create_buffer(env) as buffer:
            with buffer.publish() as publisher:
                publisher.publish_value("")
                event = AutoResetEvent()
                with buffer.subscribe(event) as subscription:
                    env.publish_observe_delay()
                    subscription.call_events(self._subscriber1)
        self._subscriber1.assert_published_values([""], self)
        self._subscriber1.assert_error_values([], self)

    # noinspection PyTypeChecker
    @parameterized(envs)
    @no_type_check
    def test_illegal_params(self, param: BufferTestParams) -> None:
        env = param._env

        with self._create_buffer(env) as buffer1:
            with buffer1.publish() as publisher1:
                with self.assertRaisesRegex(ValueError, "Timeout value must not be zero. Use TIMEOUT_NO_WAIT or TIMEOUT_INFINITE as appropriate."):
                    publisher1.publish_value("hi", timeout=0.0)
                with self.assertRaisesRegex(ValueError, "Timeout value must be greater than zero, or one of the special values TIMEOUT_NO_WAIT or TIMEOUT_INFINITE."):
                    publisher1.publish_value("hi", timeout=-1.0)

                fill_the_buffer(publisher1, BUFFER_SIZE)
                with self.assertRaisesRegex(ValueError, "Unrecognised action value in handle_unexpected_situation"):
                    publisher1.publish_value("hi", timeout=0.1, on_full_action=12345)

        with self._create_buffer(env) as buffer2:
            with buffer2.publish() as publisher2:
                with self.assertRaisesRegex(TypeError, "If an error is supplied, it must be an instance of Exception"):
                    publisher2.publish_complete(1234)

                with self.assertRaisesRegex(ValueError, "Timeout value must not be zero. Use TIMEOUT_NO_WAIT or TIMEOUT_INFINITE as appropriate."):
                    publisher2.publish_complete(None, timeout=0.0)
                with self.assertRaisesRegex(ValueError, "Timeout value must be greater than zero, or one of the special values TIMEOUT_NO_WAIT or TIMEOUT_INFINITE."):
                    publisher2.publish_complete(None, timeout=-1.0)

                fill_the_buffer(publisher2, BUFFER_SIZE)
                with self.assertRaisesRegex(ValueError, "Unrecognised action value in handle_unexpected_situation"):
                    publisher2.publish_complete(None, timeout=0.1, on_full_action=12345)

    @staticmethod
    def _create_buffer(env: BufferTestEnvironment) -> Buffer[str]:
        return env.create_buffer(str, BUFFER_SIZE, "buffer")

    def _pop(self, buffer: Observable[str], pre_sleep: float, count: int) -> float:
        first_pop_time = -1000000.0
        time.sleep(pre_sleep)
        event = AutoResetEvent()
        with buffer.subscribe(event) as subscription:
            for i in range(count):
                if i == 0:
                    first_pop_time = time.perf_counter()
                subscription.call_events(self._subscriber1)
        return first_pop_time
