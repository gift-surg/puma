import threading
import time
from unittest import TestCase

from puma.primitives import AutoResetEvent


class AutoResetEventTest(TestCase):
    # AutoResetEvent is derived from threading.Event, and only overrides the wait() method. We only test the modified behaviour, not the base class's behaviour.

    def test_cleared_if_preset(self) -> None:
        event = AutoResetEvent()
        event.set()
        self.assertTrue(event.is_set())
        self.assertTrue(event.wait(timeout=None))
        self.assertFalse(event.is_set())

    def test_cleared_if_wait_successful(self) -> None:
        event = AutoResetEvent()
        self.assertFalse(event.is_set())
        thread = threading.Thread(target=self._set_after_delay, args=[event])
        thread.start()
        try:
            t1 = time.perf_counter()
            ret = event.wait(timeout=10.0)
            t2 = time.perf_counter()
            self.assertTrue(ret)
            self.assertFalse(event.is_set())
            self.assertGreaterEqual(t2 - t1, 0.1)
            self.assertLess(t2 - t1, 1.0)
        finally:
            thread.join()

    @staticmethod
    def _set_after_delay(event: AutoResetEvent) -> None:
        time.sleep(0.1)
        event.set()
