import logging
import queue
import threading
import time
from concurrent.futures.thread import ThreadPoolExecutor
from typing import no_type_check
from unittest import TestCase

from puma.buffer import Buffer, Publisher
from puma.helpers.testing.logging.capture_logs import CaptureLogs
from puma.helpers.testing.logging.decorator import assert_no_warnings_or_errors_logged
from puma.helpers.testing.parameterized import parameterized
from puma.logging import LogLevel
from puma.primitives import AutoResetEvent
from puma.timeouts import TIMEOUT_NO_WAIT
from puma.unexpected_situation_action import UnexpectedSituationAction
from tests.buffer._parameterisation import BufferTestEnvironment, BufferTestParams, envs
from tests.buffer.test_support.buffer_api_test_support import TestSubscriber

logger = logging.getLogger(__name__)

BUFFER_SIZE = 10
TIMEOUT = 0.5
TIME_TOLERANCE = 0.3


class BufferObservableApiTest(TestCase):

    def setUp(self) -> None:
        self._subscriber1: TestSubscriber = TestSubscriber()

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_subscribe_with_event(self, param: BufferTestParams) -> None:
        env = param._env
        event = AutoResetEvent()
        with self._create_buffer(env) as buffer:
            with buffer.publish() as publisher:
                with buffer.subscribe(event) as subscription:
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(self._push, publisher, TIMEOUT, 1)
                        event.wait(timeout=TIMEOUT * 10)
                        t2 = time.perf_counter()  # time now, when event released
                        t1 = future.result()  # time when first value was pushed by executor thread
                        self.assertLessEqual(0.0, t2 - t1, "Event was released before the value was pushed")
                        self.assertGreaterEqual(TIME_TOLERANCE, t2 - t1, f"Event should have been released immediately after value pushed, took {t2 - t1}")
                    subscription.call_events(self._subscriber1)
                    self._subscriber1.assert_published_values(['v0'], self)
                    self._subscriber1.assert_error_values([], self)

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_event_works_stickily(self, param: BufferTestParams) -> None:
        env = param._env
        event = AutoResetEvent()
        with self._create_buffer(env) as buffer:
            with buffer.publish() as publisher:
                publisher.publish_value("v0", TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
                env.publish_observe_delay()
                t1 = time.perf_counter()
                with buffer.subscribe(event) as subscription:
                    event.wait(timeout=TIMEOUT)
                    t2 = time.perf_counter()
                    self.assertLess(t2 - t1, TIME_TOLERANCE, f"Wait should have returned immediately when value already pushed, took {t2 - t1}")
                    subscription.call_events(self._subscriber1)
                    self._subscriber1.assert_published_values(['v0'], self)
                    self._subscriber1.assert_error_values([], self)

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_subscribe_without_event(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            with buffer.publish() as publisher:
                with buffer.subscribe(None) as subscription:
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(self._push, publisher, TIMEOUT, 1)
                        while not self._subscriber1.published_values:
                            time.sleep(TIME_TOLERANCE / 5)
                            try:
                                subscription.call_events(self._subscriber1)
                            except queue.Empty:
                                pass  # carry on waiting
                        t2 = time.perf_counter()  # time now, when value found
                        t1 = future.result()  # time when first value was pushed by executor thread
                        self.assertLessEqual(0.0, t2 - t1, "Got a value before the value was pushed")
                        self.assertGreaterEqual(TIME_TOLERANCE, t2 - t1, f"Value should have been available immediately after value pushed, took {t2 - t1}")
                    self._subscriber1.assert_published_values(['v0'], self)
                    self._subscriber1.assert_error_values([], self)

    @parameterized(envs)
    def test_multiple_subscribes_with_event(self, param: BufferTestParams) -> None:
        env = param._env
        event = AutoResetEvent()
        with self._create_buffer(env) as buffer:
            with buffer.subscribe(event):
                with self.assertRaisesRegex(RuntimeError, "Can't subscribe, already subscribed to"):
                    buffer.subscribe(event)

    @parameterized(envs)
    def test_multiple_subscribes_without_event(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            with buffer.subscribe(None):
                with self.assertRaisesRegex(RuntimeError, "Can't subscribe, already subscribed to"):
                    buffer.subscribe(None)

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_unsubscribe(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            event = AutoResetEvent()
            subscription = buffer.subscribe(event)
            buffer.unsubscribe()
            with self.assertRaisesRegex(RuntimeError, "Subscription has been unsubscribed"):
                subscription.call_events(self._subscriber1)

    @parameterized(envs)
    def test_multiple_unsubscribes_1(self, param: BufferTestParams) -> None:
        with assert_no_warnings_or_errors_logged(self):
            env = param._env
        with self._create_buffer(env) as buffer:
            with buffer.publish() as publisher:
                self._push(publisher, 0.0, 2)
                event = AutoResetEvent()
                buffer.subscribe(event)
                buffer.unsubscribe()
                with CaptureLogs(LogLevel.warn) as log_context:
                    buffer.unsubscribe()
                    records = log_context.pop_captured_records()
                    self.assertTrue(records.containing_message("Ignoring buffer unsubscribe, not subscribed").with_levels_in({LogLevel.warn}))

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_multiple_unsubscribes_2(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            with buffer.publish() as publisher:
                self._push(publisher, 0.0, 2)
                event = AutoResetEvent()
                with buffer.subscribe(event):
                    buffer.unsubscribe()
                # no error or warning; no exception

    @parameterized(envs)
    def test_destroy_while_still_subscribed(self, param: BufferTestParams) -> None:
        with assert_no_warnings_or_errors_logged(self) as log_context_outer:
            env = param._env
            buf = self._create_buffer(env)
            buffer = buf.__enter__()
            buffer.subscribe(None)
            with log_context_outer.nested_capture_context(LogLevel.warn, shield_parent=True) as log_context:
                buf.__exit__(None, None, None)
                records = log_context.pop_captured_records()
                self.assertTrue(records.containing_message("Buffer being destroyed while still subscribed to").with_levels_in({LogLevel.warn}))

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_buffer_name(self, param: BufferTestParams) -> None:
        env = param._env
        with env.create_buffer(str, BUFFER_SIZE, "abuff_name") as buffer:
            with buffer.subscribe(None) as subscription:
                self.assertEqual("abuff_name", subscription.buffer_name())

    # noinspection PyTypeChecker
    @parameterized(envs)
    @no_type_check
    def test_illegal_params(self, param: BufferTestParams) -> None:
        env = param._env

        with self._create_buffer(env) as buffer:
            with self.assertRaisesRegex(TypeError, "If an event is supplied, it must be an AutoResetEvent"):
                buffer.subscribe(threading.Event)

    @staticmethod
    def _create_buffer(env: BufferTestEnvironment) -> Buffer[str]:
        return env.create_buffer(str, BUFFER_SIZE, "buffer")

    @staticmethod
    def _push(publisher: Publisher[str], pre_sleep: float, count: int) -> float:
        first_push_time = -100000.0
        time.sleep(pre_sleep)
        for i in range(count):
            if i == 0:
                first_push_time = time.perf_counter()
            publisher.publish_value(f"v{i}", TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
        return first_push_time
