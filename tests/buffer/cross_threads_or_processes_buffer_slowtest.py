import logging
import queue
import time
from typing import Optional
from unittest import TestCase

from puma.buffer import Observable, Publishable, TraceableException
from puma.buffer.implementation.managed_queues import ManagedQueueTypes
from puma.helpers.string import safe_str
from puma.helpers.testing.logging.decorator import assert_no_warnings_or_errors_logged
from puma.helpers.testing.parameterized import parameterized
from puma.primitives import AutoResetEvent
from tests.buffer._parameterisation import BufferTestEnvironment, BufferTestParams, envs

BUFFER_SIZE = 10
COUNT = BUFFER_SIZE * 3
TIME_TOLERANCE = 0.3
INITIAL_DELAY = 0.5
INTERVAL = 0.02
TIMEOUT = INITIAL_DELAY + (COUNT * INTERVAL) + 10.0

logger = logging.getLogger(__name__)


class _Pusher:
    def __init__(self, param: BufferTestParams, publishable: Publishable[str], count: int, slow: bool = False, publish_complete: bool = True) -> None:
        env = param._env
        self._publishable = publishable
        self._count = count
        self._slow = slow
        self._publish_complete = publish_complete
        self._active = env.create_thread_or_process(name="pusher", target=self._push_items)

    def start(self) -> None:
        self._active.start()

    def join(self, timeout: float) -> None:
        self._active.join(timeout)

    def is_alive(self) -> bool:
        return self._active.is_alive()

    def _push_items(self) -> None:
        with self._publishable.publish() as publisher:
            logger.debug("Pusher starting pushing")
            if self._slow:
                time.sleep(INITIAL_DELAY)
            try:
                for i in range(self._count):
                    t1 = time.perf_counter()
                    logger.debug("Pushing %d", i)
                    publisher.publish_value(str(i), timeout=TIMEOUT)
                    t2 = time.perf_counter()
                    if self._slow and t2 - t1 > TIME_TOLERANCE:
                        # since we are running slow, we assume the other end is not and so this publish should have been fast
                        raise RuntimeError("Test error: publishing should have been instantaneous, took %f", t2 - t1)
                    if self._slow:
                        time.sleep(INTERVAL)
                logger.debug("Pusher finished pushing")
            except Exception as ex:
                logger.error("Error in pusher: %s", safe_str(ex), exc_info=True)
                if self._publish_complete:
                    publisher.publish_complete(ex)
                else:
                    raise
            else:
                if self._publish_complete:
                    logger.debug("Pushing on_complete (None)")
                    publisher.publish_complete(None)
            logger.debug("Pusher exiting")


class _Popper:
    def __init__(self, param: BufferTestParams, observable: Observable[str], popped: ManagedQueueTypes[str], exceptions: ManagedQueueTypes[TraceableException],
                 slow: bool = False) -> None:
        env = param._env
        self._observable = observable
        self._popped = popped
        self._exceptions = exceptions
        self._slow = slow
        self._finished = False
        self._active = env.create_thread_or_process(name="popper", target=self._pop_items)

    def start(self) -> None:
        self._active.start()

    def join(self, timeout: float) -> None:
        self._active.join(timeout)

    def is_alive(self) -> bool:
        return self._active.is_alive()

    def _pop_items(self) -> None:
        if self._slow:
            time.sleep(INITIAL_DELAY)
        try:
            event = AutoResetEvent()
            with self._observable.subscribe(event) as subscription:
                while not self._finished:
                    event.wait(TIMEOUT)
                    while not self._finished:
                        try:
                            t1 = time.perf_counter()
                            subscription.call_events(self._on_value, self._on_complete)
                            t2 = time.perf_counter()
                            if self._slow and t2 - t1 > TIME_TOLERANCE:
                                # since we are running slow, we assume the other end is not and so this pop should have been fast
                                raise RuntimeError("Test error: pop should have been instantaneous, took %f", t2 - t1)
                            if self._slow:
                                time.sleep(INTERVAL)
                        except queue.Empty:
                            break
        except Exception as ex:
            logger.error("Error in popper: %s", safe_str(ex), exc_info=True)
            self._exceptions.put(TraceableException(ex))

    def _on_value(self, val: str) -> None:
        logger.debug("Popped %s", safe_str(val))
        self._popped.put(val)

    def _on_complete(self, err: Optional[BaseException]) -> None:
        logger.debug("Popped on_complete (%s)", safe_str(err))
        if err:
            self._exceptions.put(TraceableException(err))
        self._finished = True


class _SlowResubscribingPopper:
    # Keeps resubscribing all the time

    def __init__(self, param: BufferTestParams, observable: Observable[str], popped: ManagedQueueTypes[str], exceptions: ManagedQueueTypes[TraceableException]) -> None:
        env = param._env
        self._observable = observable
        self._popped = popped
        self._exceptions = exceptions
        self._finished = False
        self._active = env.create_thread_or_process(name="slow-popper", target=self._pop_items)

    def start(self) -> None:
        self._active.start()

    def join(self, timeout: float) -> None:
        self._active.join(timeout)

    def is_alive(self) -> bool:
        return self._active.is_alive()

    def _pop_items(self) -> None:
        try:
            time.sleep(INITIAL_DELAY)
            while not self._finished:
                event = AutoResetEvent()
                logger.debug("Slow-Popper subscribing")
                with self._observable.subscribe(event) as subscription:
                    event.wait(INTERVAL)
                    for loop in range(BUFFER_SIZE // 2):
                        if self._finished:
                            break
                        try:
                            subscription.call_events(self._on_value, self._on_complete)
                            time.sleep(INTERVAL)
                        except queue.Empty:
                            break
            logger.debug("Slow-Popper finished")
        except Exception as ex:
            logger.error("Error in slow-popper: %s", safe_str(ex), exc_info=True)
            self._exceptions.put(TraceableException(ex))

    def _on_value(self, val: str) -> None:
        logger.debug("Slow-Popper popped %s", val)
        self._popped.put(val)

    def _on_complete(self, err: Optional[BaseException]) -> None:
        logger.debug("Slow-Popper popped on_complete(%s)", safe_str(err))
        if err:
            self._exceptions.put(TraceableException(err))
        self._finished = True


class CrossThreadsOrProcessesBufferSlowTest(TestCase):
    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_slow_pusher(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_item_queue(env) as popped, \
                self._create_exception_queue(env) as exceptions:
            with env.create_buffer(str, BUFFER_SIZE, "buffer") as buffer:
                pusher = _Pusher(param, buffer, COUNT, slow=True)
                popper = _Popper(param, buffer, popped, exceptions)
                pusher.start()
                popper.start()
                pusher.join(TIMEOUT)
                self.assertFalse(pusher.is_alive())
                popper.join(TIMEOUT)
                self.assertFalse(popper.is_alive())
                for i in range(COUNT):
                    self.assertEqual(str(i), popped.get_nowait())
                self.assertTrue(exceptions.empty())

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_slow_popper(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_item_queue(env) as popped, \
                self._create_exception_queue(env) as exceptions:
            with env.create_buffer(str, BUFFER_SIZE, "buffer") as buffer:
                pusher = _Pusher(param, buffer, COUNT)
                popper = _Popper(param, buffer, popped, exceptions, slow=True)
                pusher.start()
                popper.start()
                pusher.join(TIMEOUT)
                self.assertFalse(pusher.is_alive())
                popper.join(TIMEOUT)
                self.assertFalse(popper.is_alive())
                for i in range(COUNT):
                    self.assertEqual(str(i), popped.get_nowait())
                self.assertTrue(exceptions.empty())

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_subscribe_while_push_blocked(self, param: BufferTestParams) -> None:
        # Tests that a blocking call to publish() (waiting for space to be available in the queue) does not prevent another thread/process from subscribing.
        # Regression test: this issue was discovered late in the day
        env = param._env
        with self._create_item_queue(env) as popped, \
                self._create_exception_queue(env) as exceptions:
            with env.create_buffer(str, BUFFER_SIZE, "buffer") as buffer:
                pusher = _Pusher(param, buffer, COUNT)
                popper = _SlowResubscribingPopper(param, buffer, popped, exceptions)
                popper.start()
                pusher.start()
                pusher.join(TIMEOUT)
                self.assertFalse(pusher.is_alive())
                popper.join(TIMEOUT)
                self.assertFalse(popper.is_alive())
                for i in range(COUNT):
                    self.assertEqual(str(i), popped.get_nowait())
                self.assertTrue(exceptions.empty())

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_with_multiple_publishers(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_item_queue(env) as popped, \
                self._create_exception_queue(env) as exceptions:
            with env.create_buffer(str, BUFFER_SIZE, "buffer") as buffer:
                pushers = [_Pusher(param, buffer, COUNT, slow=True, publish_complete=False) for _ in range(10)]
                popper = _Popper(param, buffer, popped, exceptions)
                popper.start()
                for pusher in pushers:
                    pusher.start()

                for pusher in pushers:
                    pusher.join(TIMEOUT)
                    self.assertFalse(pusher.is_alive())

                with buffer.publish() as publisher:
                    publisher.publish_complete(None)

                popper.join(TIMEOUT)
                self.assertFalse(popper.is_alive())

                counts = {str(i): 0 for i in range(COUNT)}
                while True:
                    try:
                        val = popped.get_nowait()
                        counts[val] += 1
                    except queue.Empty:
                        break
                for i in range(COUNT):
                    self.assertEqual(len(pushers), counts[str(i)])

                self.assertTrue(exceptions.empty())

    @staticmethod
    def _create_item_queue(env: BufferTestEnvironment) -> ManagedQueueTypes[str]:
        return env.create_managed_queue(str, name="items")

    @staticmethod
    def _create_exception_queue(env: BufferTestEnvironment) -> ManagedQueueTypes[TraceableException]:
        return env.create_managed_queue(TraceableException, name="exceptions")
