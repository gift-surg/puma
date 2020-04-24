import queue
import random
import threading
import time
from typing import List
from unittest import TestCase

from puma.buffer.implementation.managed_queues import ManagedThreadQueue
from puma.primitives import AutoResetEvent, ThreadCondition

COUNT = 100


class AutoResetEventTest(TestCase):
    def setUp(self) -> None:
        self._event = AutoResetEvent()
        self._send_queue = ManagedThreadQueue[int]()
        self._send_queue.__enter__()
        self._received: List[int] = []
        self._receive_condition = ThreadCondition()

    def tearDown(self) -> None:
        self._send_queue.__exit__(None, None, None)

    def test_single_waiting_thread(self) -> None:
        self._threaded_send_receive(threads=1)

    def test_multiple_waiting_threads(self) -> None:
        self._threaded_send_receive(threads=10)

    def _threaded_send_receive(self, threads: int) -> None:
        send_thread = threading.Thread(target=self._sender)
        receive_threads = [threading.Thread(target=self._receiver) for _ in range(threads)]
        for receive_thread in receive_threads:
            receive_thread.start()
        send_thread.start()
        send_thread.join(10.0)
        self._stop_receivers(threads)
        for receive_thread in receive_threads:
            receive_thread.join(1.0)
        self.assertEqual(self._received, list(range(COUNT)))

    def _send_value(self, val: int) -> None:
        self._send_queue.put_nowait(val)
        self._event.set()

    def _sender(self) -> None:
        for i in range(COUNT):
            self._send_value(i)
            time.sleep(random.uniform(0.001, 0.01))

    def _stop_receivers(self, num_receiving_threads: int) -> None:
        for _ in range(num_receiving_threads):
            self._send_value(-1)

    def _receiver(self) -> None:
        while True:
            self._event.wait()
            with self._receive_condition:  # so that only one receiver thread pops data off the queue at a time
                while True:
                    try:
                        val = self._send_queue.get_nowait()
                        if val < 0:
                            return
                        self._received.append(val)
                    except queue.Empty:
                        break
