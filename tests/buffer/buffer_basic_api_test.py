import logging
import queue
import time
from concurrent.futures.thread import ThreadPoolExecutor
from typing import List, TypeVar, cast, no_type_check
from unittest import TestCase

from puma.buffer import Buffer, Observable, Publisher
from puma.primitives import AutoResetEvent
from puma.timeouts import TIMEOUT_NO_WAIT
from puma.unexpected_situation_action import UnexpectedSituationAction
from tests.buffer._parameterisation import BufferTestEnvironment, BufferTestParams, envs
from tests.buffer.test_support.buffer_api_test_support import TestSubscriber, TestSubscriberBase, fill_the_buffer, publish_values, publish_values_and_complete, receive_all
from tests.parameterized import parameterized
from tests.test_logging_helpers import assert_no_warnings_or_errors_logged

logger = logging.getLogger(__name__)

BUFFER_SIZE = 10
TIMEOUT = 0.5
TIME_TOLERANCE = 0.3
POP_TIMEOUT = 0.1


class Super:
    a: int


class Derived(Super):
    b: int


Type = TypeVar("Type")


class BufferBasicApiTest(TestCase):

    def setUp(self) -> None:
        self._subscriber1: TestSubscriber = TestSubscriber()

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_simple_push_pop_with_event(self, param: BufferTestParams) -> None:
        for trials in range(3):
            env = param._env
            items = [str(i) for i in range(trials)]
            subscriber: TestSubscriber = TestSubscriber()
            event = AutoResetEvent()
            with self._create_buffer(env) as buffer:
                with buffer.subscribe(event) as subscription:
                    publish_values_and_complete(buffer, items)
                    env.publish_observe_delay()
                    receive_all(subscription, event, subscriber)

            subscriber.assert_published_values(items, self)
            subscriber.assert_completed(True, self)
            subscriber.assert_error_values([], self)

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_simple_push_pop_without_event(self, param: BufferTestParams) -> None:
        for trials in range(3):
            env = param._env
            items = [str(i) for i in range(trials)]
            subscriber: TestSubscriber = TestSubscriber()
            with self._create_buffer(env) as buffer:
                with buffer.subscribe(None) as subscription:
                    publish_values_and_complete(buffer, items)
                    env.publish_observe_delay()
                    receive_all(subscription, None, subscriber)

            subscriber.assert_published_values(items, self)
            subscriber.assert_completed(True, self)
            subscriber.assert_error_values([], self)

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_push_pop_without_event(self, param: BufferTestParams) -> None:
        env = param._env
        expected: List[str] = []
        with self._create_buffer(env) as buffer:
            with buffer.publish() as publisher, buffer.subscribe(None) as subscription:
                for fills in range(2):
                    for thing in range(BUFFER_SIZE):
                        val = str(thing)
                        expected.append(val)
                        publisher.publish_value(val, TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
                    env.publish_observe_delay()
                    receive_all(subscription, None, self._subscriber1)

        self._subscriber1.assert_published_values(expected, self)
        self._subscriber1.assert_completed(False, self)
        self._subscriber1.assert_error_values([], self)

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_unsubscribe_subscribe_does_not_lose_data(self, param: BufferTestParams) -> None:
        items = [str(i) for i in range(BUFFER_SIZE // 2)]

        env = param._env
        with self._create_buffer(env) as buffer:
            with buffer.publish() as publisher:
                publish_values(publisher, items)
                with buffer.subscribe(None) as subscription:
                    receive_all(subscription, None, self._subscriber1)
                publish_values(publisher, items)
                with buffer.subscribe(None) as subscription:
                    receive_all(subscription, None, self._subscriber1)
                with buffer.subscribe(None) as subscription:
                    env.publish_observe_delay()
                    receive_all(subscription, None, self._subscriber1)

        self._subscriber1.assert_published_values([*items, *items], self)
        self._subscriber1.assert_completed(False, self)
        self._subscriber1.assert_error_values([], self)

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_empty(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            with buffer.subscribe(None) as subscription:
                env.publish_observe_delay()
                receive_all(subscription, None, self._subscriber1)

        self._subscriber1.assert_published_values([], self)
        self._subscriber1.assert_completed(False, self)
        self._subscriber1.assert_error_values([], self)

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_full_when_not_subscribed(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            with buffer.publish() as publisher:
                fill_the_buffer(publisher, BUFFER_SIZE)
                env.publish_observe_delay()

                with self.assertRaises(queue.Full):
                    publisher.publish_value("excess", TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)

                with self.assertRaises(queue.Full):
                    publisher.publish_complete(None, TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_full_when_subscribed(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            with buffer.publish() as publisher:
                with buffer.subscribe(None):
                    fill_the_buffer(publisher, BUFFER_SIZE)
                    env.publish_observe_delay()

                    with self.assertRaises(queue.Full):
                        publisher.publish_value("excess", TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)

                    with self.assertRaises(queue.Full):
                        publisher.publish_complete(None, TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_subscribe_pop_unsubscribe_during_push_does_not_block(self, param: BufferTestParams) -> None:
        env = param._env

        with self._create_buffer(env) as buffer:
            with buffer.publish() as publisher:
                fill_the_buffer(publisher, BUFFER_SIZE)
                env.publish_observe_delay()
                with ThreadPoolExecutor(max_workers=1) as executor:
                    future = executor.submit(self._pop, buffer, TIMEOUT, 1)
                    publisher.publish_value("excess", TIMEOUT * 10, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
                    t2 = time.perf_counter()  # time now, when value published
                    times = future.result()
                    t1a = times[0]  # time before value was popped by executor thread
                    t1b = times[1]  # time after value was popped by executor thread
                    self.assertLessEqual(0.0, t2 - t1a, "Published before space was freed in the buffer")
                    self.assertGreaterEqual(TIME_TOLERANCE, t2 - t1a, f"Should have pushed immediately after space was made, took {t2 - t1a}")
                    self.assertGreaterEqual(TIME_TOLERANCE, t1b - t1a, f"Popping should have been instantaneous, took {t1b - t1a}")

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_push_during_event_wait_does_not_block(self, param: BufferTestParams) -> None:
        env = param._env

        event = AutoResetEvent()
        with self._create_buffer(env) as buffer:
            with buffer.publish() as publisher:
                with buffer.subscribe(event):
                    with ThreadPoolExecutor(max_workers=1) as executor:
                        future = executor.submit(self._push, publisher, TIMEOUT, 1)
                        event.wait(timeout=TIMEOUT * 10)
                        t2 = time.perf_counter()  # time now, when event released
                        times = future.result()
                        t1a = times[0]  # time before value was pushed by executor thread
                        t1b = times[1]  # time after value was pushed by executor thread
                        self.assertLessEqual(0.0, t2 - t1a, "Event was released before the value was pushed")
                        self.assertGreaterEqual(TIME_TOLERANCE, t2 - t1a, f"Event should have been released immediately after value pushed, took {t2 - t1a}")
                        self.assertGreaterEqual(TIME_TOLERANCE, t1b - t1a, f"Pushing should have been instantaneous, took {t1b - t1a}")

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_contravariance(self, param: BufferTestParams) -> None:
        env = param._env

        # noinspection PyUnusedLocal
        with env.create_buffer(Super, BUFFER_SIZE, "buffer") as buffer:
            with buffer.publish() as publisher:
                event = AutoResetEvent()
                with buffer.subscribe(event) as subscription:
                    subscriber = TestSubscriberBase[Super]()

                    x: Derived = Derived()
                    x.a = 1
                    x.b = 2
                    publisher.publish_value(x, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
                    env.publish_observe_delay()
                    receive_all(subscription, event, subscriber)
                    self.assertEqual(1, len(subscriber.published_values))
                    y: Super = subscriber.published_values[0]

                    self.assertTrue(isinstance(y, Derived))
                    yy: Derived = cast(Derived, y)
                    self.assertEqual(2, yy.b)

    # noinspection PyTypeChecker
    @parameterized(envs)
    @no_type_check
    def test_illegal_params(self, param: BufferTestParams) -> None:
        env = param._env

        with self.assertRaisesRegex(RuntimeError, "Buffer must be created with a size of a least 1"):
            env.create_buffer(str, 0, "buffer")
        with self.assertRaisesRegex(RuntimeError, "A name must be supplied"):
            env.create_buffer(str, 1, None)
        with self.assertRaisesRegex(RuntimeError, "A name must be supplied"):
            env.create_buffer(str, 1, "")

    def _pop(self, buffer: Observable[str], pre_sleep: float, count: int) -> List[float]:
        time.sleep(pre_sleep)
        ret = []
        event = AutoResetEvent()
        with buffer.subscribe(event) as subscription:
            for i in range(count):
                if not event.wait(POP_TIMEOUT):
                    break
                ret.append(time.perf_counter())
                try:
                    subscription.call_events(self._subscriber1)
                except queue.Empty:
                    logger.error("Buffer unexpectedly empty")
                    break
                ret.append(time.perf_counter())
        return ret

    @staticmethod
    def _create_buffer(env: BufferTestEnvironment) -> Buffer[str]:
        return env.create_buffer(str, BUFFER_SIZE, "buffer")

    @staticmethod
    def _push(publisher: Publisher[str], pre_sleep: float, count: int) -> List[float]:
        time.sleep(pre_sleep)
        ret = []
        for i in range(count):
            ret.append(time.perf_counter())
            publisher.publish_value(f"v{i}", TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            ret.append(time.perf_counter())
        return ret
