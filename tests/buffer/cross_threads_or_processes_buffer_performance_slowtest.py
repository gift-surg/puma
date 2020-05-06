import logging
import queue
import time
from enum import Enum, auto, unique
from statistics import mean
from typing import List, Optional
from unittest import TestCase

from puma.buffer import Buffer, TraceableException
from puma.buffer.implementation.managed_queues import ManagedQueueTypes
from puma.helpers.string import safe_str
from puma.helpers.testing.parameterized import parameterized
from puma.primitives import AutoResetEvent, EventType
from tests.buffer._parameterisation import BufferTestEnvironment, BufferTestParams, envs

BUFFER_SIZE = 100
COUNT = BUFFER_SIZE * 3
INTERVAL = 0.005
TIMEOUT = (COUNT * INTERVAL) + 10.0

logger = logging.getLogger(__name__)


@unique
class PerformanceInfo(Enum):
    Latency = auto()
    Performance = auto()


class _Pusher:
    def __init__(self, param: BufferTestParams, buffer: Buffer[str], count: int, *, interval: float) -> None:
        env = param._env
        self._buffer = buffer
        self._count = count
        self._interval = interval
        self._active = env.create_thread_or_process(name="pusher", target=self._push_items)

    def start(self) -> None:
        self._active.start()

    def join(self, timeout: float) -> None:
        self._active.join(timeout)

    def is_alive(self) -> bool:
        return self._active.is_alive()

    def _push_items(self) -> None:
        with self._buffer.publish() as publisher:
            try:
                for i in range(self._count):
                    timestamp = time.monotonic()
                    publisher.publish_value(str(timestamp), timeout=TIMEOUT)
                    time.sleep(self._interval)
            except Exception as ex:
                logger.error("Error in pusher: %s", safe_str(ex), exc_info=True)
                publisher.publish_complete(ex)
            else:
                publisher.publish_complete(None)


class _Popper:
    def __init__(self, param: BufferTestParams, buffer: Buffer[str],
                 numbers: ManagedQueueTypes[float], info_type: PerformanceInfo,
                 exceptions: ManagedQueueTypes[TraceableException],
                 popper_running: EventType) -> None:
        env = param._env
        self._buffer = buffer
        self._numbers = numbers
        self._info_type = info_type
        self._exceptions = exceptions
        self._popper_running = popper_running
        self._finished = False
        self._active = env.create_thread_or_process(name="popper", target=self._pop_items)
        self._first_timestamp: Optional[float] = None
        self._last_timestamp: Optional[float] = None

    def start(self) -> None:
        self._active.start()

    def join(self, timeout: float) -> None:
        self._active.join(timeout)

    def is_alive(self) -> bool:
        return self._active.is_alive()

    def _pop_items(self) -> None:
        try:
            self._popper_running.set()
            event = AutoResetEvent()
            with self._buffer.subscribe(event) as subscription:
                while not self._finished:
                    event.wait(TIMEOUT)
                    while not self._finished:
                        try:
                            subscription.call_events(self._on_value, self._on_complete)
                        except queue.Empty:
                            break
        except Exception as ex:
            logger.error("Error in popper: %s", safe_str(ex), exc_info=True)
            self._exceptions.put(TraceableException(ex))

    def _on_value(self, val: str) -> None:
        timestamp_received = float(val)
        timestamp_now = time.monotonic()
        if self._info_type == PerformanceInfo.Performance and self._first_timestamp is None:
            self._first_timestamp = timestamp_received
        if self._info_type == PerformanceInfo.Latency:
            self._numbers.put(timestamp_now - timestamp_received)
        self._last_timestamp = timestamp_received

    def _on_complete(self, err: Optional[BaseException]) -> None:
        if err:
            self._exceptions.put(TraceableException(err))
        if self._info_type == PerformanceInfo.Performance:
            assert self._first_timestamp
            assert self._last_timestamp
            self._numbers.put(self._first_timestamp)
            self._numbers.put(self._last_timestamp)
        self._finished = True


class CrossThreadsOrProcessesBufferPerformanceSlowTest(TestCase):

    @parameterized(envs)
    def test_latency_performance(self, param: BufferTestParams) -> None:
        env = param._env
        print()
        with self._create_float_queue(env) as latencies, self._create_exception_queue(env) as exceptions:
            with env.create_buffer(str, BUFFER_SIZE, "buffer") as buffer:
                popper_running = env.create_event()
                pusher = _Pusher(param, buffer, COUNT, interval=INTERVAL)
                popper = _Popper(param, buffer, latencies, PerformanceInfo.Latency, exceptions, popper_running)
                popper.start()

                self.assertTrue(popper_running.wait(TIMEOUT))
                pusher.start()

                pusher.join(TIMEOUT)
                self.assertTrue(exceptions.empty())
                self.assertFalse(pusher.is_alive())
                popper.join(TIMEOUT)
                self.assertFalse(popper.is_alive())
                self.assertTrue(exceptions.empty())

                latency_list: List[float] = []
                while len(latency_list) < COUNT:
                    latency: float = latencies.get(timeout=0.05)
                    latency_list.append(latency)
                self.assertEqual(COUNT, len(latency_list))
                min_latency = min(latency_list)
                max_latency = max(latency_list)
                mean_latency = mean(latency_list)
                print(f"Max latency: {max_latency}; Mean latency: {mean_latency}")
                if min_latency < 0.0:
                    raise RuntimeError("Test error: negative latency")
                if max_latency > env.get_max_latency():
                    raise RuntimeError("Max latency was too high: %f", max_latency)
                if mean_latency > env.get_max_mean_latency():
                    raise RuntimeError("Mean latency was too high: %f", mean_latency)

    @parameterized(envs)
    def test_throughput_performance(self, param: BufferTestParams) -> None:
        env = param._env
        print()
        with self._create_float_queue(env) as timestamps, self._create_exception_queue(env) as exceptions:
            with env.create_buffer(str, BUFFER_SIZE, "buffer") as buffer:
                popper_running = env.create_event()
                pusher = _Pusher(param, buffer, COUNT, interval=0.0)
                popper = _Popper(param, buffer, timestamps, PerformanceInfo.Performance, exceptions, popper_running)
                popper.start()

                self.assertTrue(popper_running.wait(TIMEOUT))
                pusher.start()

                pusher.join(TIMEOUT)
                self.assertTrue(exceptions.empty())
                self.assertFalse(pusher.is_alive())
                popper.join(TIMEOUT)
                self.assertFalse(popper.is_alive())
                self.assertTrue(exceptions.empty())

                first_timestamp: float = timestamps.get(timeout=0.05)
                last_timestamp: float = timestamps.get(timeout=0.05)

                total_interval = last_timestamp - first_timestamp
                items_per_second = (COUNT - 1) / (total_interval or 0.015)
                # minus one because COUNT values have COUNT-1 time intervals between them
                # test for zero because on Windows, monotonic() has 15ms resolution

                print(f"Throughput {items_per_second} items per second")
                if items_per_second < 0.0:
                    raise RuntimeError("Test error: negative throughput")
                if items_per_second < env.get_min_throughput():
                    raise RuntimeError("Throughput was too low: %f", items_per_second)

    @staticmethod
    def _create_float_queue(env: BufferTestEnvironment) -> ManagedQueueTypes[float]:
        return env.create_managed_queue(float)

    @staticmethod
    def _create_exception_queue(env: BufferTestEnvironment) -> ManagedQueueTypes[TraceableException]:
        return env.create_managed_queue(TraceableException)
