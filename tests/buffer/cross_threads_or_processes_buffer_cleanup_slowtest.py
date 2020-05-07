import multiprocessing
import queue
import time
from typing import Any, List
from unittest import TestCase

from puma.attribute import copied, unmanaged
from puma.buffer import Buffer, MultiProcessBuffer, MultiThreadBuffer, Observable, Publishable, Subscription
from puma.buffer.implementation.managed_queues import ManagedQueueTypes
from puma.helpers.testing.logging.capture_logs import CaptureLogs
from puma.helpers.testing.logging.decorator import assert_no_warnings_or_errors_logged
from puma.helpers.testing.mixin import NotATestCase
from puma.helpers.testing.parameterized import parameterized
from puma.logging import LogLevel
from puma.precision_timestamp.precision_timestamp import precision_timestamp
from tests.buffer._parameterisation import BufferTestEnvironment, BufferTestParams, envs
from tests.buffer.test_support.buffer_api_test_support import TestSubscriberBase
from tests.runnable.test_support.testval import TestVal

BUFFER_SIZE = 10
DISCARD_DELAY = 3.0
LONG_DELAY = 30.0
VALUE = 777
HUGE_BUFFER_SIZE = 50000
NUM_ITEMS_TO_DISCARD_NOT_SET_ERROR_MSG: str = 'not set to keep track of number of discarded items'
NUM_ITEMS_TO_DISCARD_NOT_SET: int = -192  # dummy impossible number


class CrossThreadsOrProcessesBufferCleanupSlowTest(TestCase):

    @parameterized(envs)
    def test_cleaned_up_after_delay_when_last_publisher_unpublished_and_no_subscriber(self, param: BufferTestParams) -> None:
        # By default, a buffer will log warnings upon discarding items. This test represents the default case.
        # However this behaviour can be modified by resetting the warn_on_discard flag. See the following test:
        # test_cleaned_up_after_delay_when_last_publisher_unpublished_and_no_subscriber_without_warning
        env = param._env
        with CaptureLogs(LogLevel.warn) as log_context:
            with self._create_buffer(env) as buffer:
                num_values = 2  # TODO - this test fails on Linux with num_values = 1, so using 2 here
                for _ in range(num_values):
                    self._publish_value(buffer)  # There's no point trying to run this in a separate process because that process will simply block until the discard thread ends
                time.sleep(DISCARD_DELAY + 2.0)
                with buffer.subscribe(None) as subscription:
                    with self.assertRaises(queue.Empty):
                        subscription.call_events(lambda val: None)
            records = log_context.pop_captured_records()
            self.assertTrue(records.containing_message(f"buffer Discard thread: Discarded {num_values} items from the buffer").with_levels_in({LogLevel.warn}))

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_cleaned_up_after_delay_when_last_publisher_unpublished_and_no_subscriber_without_warning(self, param: BufferTestParams) -> None:
        # This test represents the non-default case where a buffer does NOT log warnings upon discarding items. The default case
        # is tested using: test_cleaned_up_after_delay_when_last_publisher_unpublished_and_no_subscriber
        env = param._env
        num_values = 2  # TODO (copy-pasted from default case test) - this test fails on Linux with num_values = 1, so using 2 here
        with self._create_buffer(env, warn_on_discard=False, num_items_to_discard=num_values) as buffer:
            for _ in range(num_values):
                self._publish_value(buffer)  # There's no point trying to run this in a separate process because that process will simply block until the discard thread ends
            time.sleep(DISCARD_DELAY + 2.0)
            with buffer.subscribe(None) as subscription:
                with self.assertRaises(queue.Empty):
                    subscription.call_events(lambda val: None)
            buffer.validate_num_discarded_items(self)  # type: ignore
            # Ignored type: _create_buffer returns either TestMultiThreadedBuffer or TestMultiProcessBuffer, both of which have a validate_num_discarded_items method

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_not_cleaned_up_too_soon_after_last_publisher_unpublished_and_no_subscriber(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            self._publish_value(buffer)  # There's no point trying to run this in a separate process because that process will simply block until the discard thread ends
            time.sleep(DISCARD_DELAY - 1.0)
            with buffer.subscribe(None) as subscription:
                got = self._receive_all(subscription)
                self.assertEqual([VALUE], got)

    @parameterized(envs)
    def test_cleaned_up_after_delay_when_subscriber_unsubscribed_and_no_publishers(self, param: BufferTestParams) -> None:
        # By default, a buffer will log warnings upon discarding items. This test represents the default case.
        # However this behaviour can be modified by resetting the warn_on_discard flag. See the following test:
        # test_cleaned_up_after_delay_when_subscriber_unsubscribed_and_no_publishers_without_warning
        env = param._env
        with CaptureLogs(LogLevel.warn) as log_context:
            with self._create_buffer(env) as buffer:
                with buffer.subscribe(None):
                    self._publish_value(buffer)  # There's no point trying to run this in a separate process because that process will simply block until the discard thread ends
                time.sleep(DISCARD_DELAY + 1.0)
                with buffer.subscribe(None) as subscription:
                    with self.assertRaises(queue.Empty):
                        subscription.call_events(lambda val: None)
            records = log_context.pop_captured_records()
            self.assertTrue(records.containing_message("buffer Discard thread: Discarded 1 items from the buffer").with_levels_in({LogLevel.warn}))

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_cleaned_up_after_delay_when_subscriber_unsubscribed_and_no_publishers_without_warning(self, param: BufferTestParams) -> None:
        # This test represents the non-default case where a buffer does NOT log warnings upon discarding items. The default case
        # is tested using: test_cleaned_up_after_delay_when_subscriber_unsubscribed_and_no_publishers
        env = param._env
        with self._create_buffer(env, warn_on_discard=False, num_items_to_discard=1) as buffer:
            with buffer.subscribe(None):
                self._publish_value(buffer)  # There's no point trying to run this in a separate process because that process will simply block until the discard thread ends
            time.sleep(DISCARD_DELAY + 1.0)
            with buffer.subscribe(None) as subscription:
                with self.assertRaises(queue.Empty):
                    subscription.call_events(lambda val: None)
            buffer.validate_num_discarded_items(self)  # type: ignore
            # Ignored type: _create_buffer returns either TestMultiThreadedBuffer or TestMultiProcessBuffer, both of which have a validate_num_discarded_items method

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_not_cleaned_up_too_soon_when_subscriber_unsubscribed_and_no_publishers(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            with buffer.subscribe(None):
                self._publish_value(buffer)  # There's no point trying to run this in a separate process because that process will simply block until the discard thread ends
            time.sleep(DISCARD_DELAY - 1.0)
            with buffer.subscribe(None) as subscription:
                got = self._receive_all(subscription)
                self.assertEqual([VALUE], got)

    @parameterized(envs)
    def test_not_cleaned_up_when_still_got_a_publisher_and_no_subscriber(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            publisher1 = env.create_thread_or_process(name='publisher1', target=self._publish_sleep_then_publish_values, args=(DISCARD_DELAY + 2.0, buffer, 1))
            publisher2 = env.create_thread_or_process(name='publisher2', target=self._publish_sleep_then_publish_values, args=(0.0, buffer, 1))
            publisher1.start()
            publisher2.start()
            publisher2.join(LONG_DELAY)
            self.assertFalse(publisher2.is_alive())
            time.sleep(DISCARD_DELAY + 1.0)
            with buffer.subscribe(None) as subscription:
                got = self._receive_all(subscription)
                self.assertEqual([VALUE], got)  # publisher1 hasn't published yet
            publisher1.join(LONG_DELAY)
            self.assertFalse(publisher1.is_alive())

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_not_cleaned_up_when_still_got_a_subscriber_and_no_publishers(self, param: BufferTestParams) -> None:
        env = param._env
        with self._create_buffer(env) as buffer:
            with env.create_managed_queue(int, 10, 'results') as result_queue:
                background_subscriber = env.create_thread_or_process(name='subscriber', target=self._subscribe_sleep_capture, args=(DISCARD_DELAY + 2.0, buffer, result_queue))
                background_subscriber.start()
                self._publish_value(buffer)
                background_subscriber.join(LONG_DELAY)
                self.assertFalse(background_subscriber.is_alive())
                results = self._pop_all(result_queue)
                self.assertEqual([VALUE], results)

    @assert_no_warnings_or_errors_logged
    def test_process_not_blocked_if_buffer_empty(self) -> None:
        with TestMultiProcessBuffer(BUFFER_SIZE, "buffer", discard_delay=LONG_DELAY) as buffer:
            background_publisher = multiprocessing.Process(name='publisher', target=self._publish_sleep_then_publish_values, args=(1.0, buffer, 1))
            background_subscriber = multiprocessing.Process(name='subscriber', target=self._subscribe_sleep_discard, args=(2.0, buffer))
            background_publisher.start()
            background_subscriber.start()
            t1 = time.monotonic()
            background_publisher.join(LONG_DELAY)
            self.assertFalse(background_publisher.is_alive())
            background_subscriber.join(LONG_DELAY)
            self.assertFalse(background_subscriber.is_alive())
            t2 = time.monotonic()
            self.assertLess(t2 - t1, LONG_DELAY)

    @assert_no_warnings_or_errors_logged
    def test_process_not_blocked_if_buffer_empty_large_items(self) -> None:
        with TestMultiProcessBuffer(BUFFER_SIZE, "buffer", discard_delay=LONG_DELAY) as buffer:
            background_publisher = multiprocessing.Process(name='publisher', target=self._publish_sleep_then_publish_large_values, args=(1.0, buffer, BUFFER_SIZE - 1))
            background_subscriber = multiprocessing.Process(name='subscriber', target=self._subscribe_sleep_discard, args=(2.0, buffer))
            background_publisher.start()
            background_subscriber.start()
            t1 = time.monotonic()
            background_publisher.join(LONG_DELAY)
            self.assertFalse(background_publisher.is_alive())
            background_subscriber.join(LONG_DELAY)
            self.assertFalse(background_subscriber.is_alive())
            t2 = time.monotonic()
            self.assertLess(t2 - t1, LONG_DELAY)

    @assert_no_warnings_or_errors_logged
    def test_process_not_blocked_if_buffer_never_published_to(self) -> None:
        with TestMultiProcessBuffer(BUFFER_SIZE, "buffer", discard_delay=LONG_DELAY) as buffer:
            background_publisher = multiprocessing.Process(name='publisher', target=self._publish_sleep, args=(1.0, buffer))
            background_subscriber = multiprocessing.Process(name='subscriber', target=self._subscribe_sleep, args=(1.0, buffer))
            background_publisher.start()
            background_subscriber.start()
            t1 = time.monotonic()
            background_publisher.join(LONG_DELAY)
            self.assertFalse(background_publisher.is_alive())
            background_subscriber.join(LONG_DELAY)
            self.assertFalse(background_subscriber.is_alive())
            t2 = time.monotonic()
            self.assertLess(t2 - t1, LONG_DELAY)

    @staticmethod
    def _create_buffer(env: BufferTestEnvironment, **kwargs: Any) -> Buffer[TestVal]:
        if env.descriptive_name() == "MultiThreaded":
            return TestMultiThreadedBuffer(BUFFER_SIZE, "buffer", **kwargs)
        elif env.descriptive_name() == "MultiProcess":
            return TestMultiProcessBuffer(BUFFER_SIZE, "buffer", **kwargs)
        else:
            raise RuntimeError("Unknown environment")

    @classmethod
    def _publish_value(cls, publishable: Publishable[TestVal]) -> None:
        with publishable.publish() as publisher:
            publisher.publish_value(TestVal(VALUE, 0.0))

    @classmethod
    def _publish_sleep(cls, delay: float, publishable: Publishable[TestVal]) -> None:
        with publishable.publish():
            time.sleep(delay)

    @classmethod
    def _publish_sleep_then_publish_values(cls, delay: float, publishable: Publishable[TestVal], count: int) -> None:
        with publishable.publish() as publisher:
            time.sleep(delay)
            for i in range(count):
                publisher.publish_value(TestVal(VALUE, precision_timestamp()))

    @classmethod
    def _publish_sleep_then_publish_large_values(cls, delay: float, publishable: Publishable[TestVal], count: int) -> None:
        with publishable.publish() as publisher:
            time.sleep(delay)
            for i in range(count):
                publisher.publish_value(TestVal(VALUE, precision_timestamp(), large=True))

    @classmethod
    def _subscribe_sleep(cls, delay: float, observable: Observable[TestVal]) -> None:
        with observable.subscribe(None):
            time.sleep(delay)

    @classmethod
    def _subscribe_sleep_capture(cls, delay: float, observable: Observable[TestVal], results_queue: ManagedQueueTypes[int]) -> None:
        with observable.subscribe(None):
            time.sleep(delay)
        with observable.subscribe(None) as subscription:
            received = cls._receive_all(subscription)
        for item in received:
            results_queue.put_nowait(item)

    @classmethod
    def _subscribe_sleep_discard(cls, delay: float, observable: Observable[TestVal]) -> None:
        with observable.subscribe(None):
            time.sleep(delay)
        with observable.subscribe(None) as subscription:
            cls._receive_all(subscription)

    @classmethod
    def _receive_all(cls, subscription: Subscription[TestVal]) -> List[int]:
        subscriber: TestSubscriberBase[TestVal] = TestSubscriberBase[TestVal]()
        while True:
            try:
                subscription.call_events(subscriber)
            except queue.Empty:
                break
        if subscriber.error_values:
            raise RuntimeError("Errors received")
        return [v.counter for v in subscriber.published_values]

    @classmethod
    def _pop_all(cls, q: ManagedQueueTypes[int]) -> List[int]:
        ret = []
        while True:
            try:
                ret.append(q.get_nowait())
            except queue.Empty:
                break
        return ret


class TestMultiThreadedBuffer(MultiThreadBuffer[TestVal], NotATestCase):
    _discard_delay: float = copied("_discard_delay")
    _num_items_to_discard: int = copied("_num_items_to_discard")
    _num_discarded_items: int = unmanaged("_num_discarded_items")

    def __init__(self, max_size: int, name: str, *, discard_delay: float = DISCARD_DELAY, warn_on_discard: bool = True, num_items_to_discard: int = NUM_ITEMS_TO_DISCARD_NOT_SET):
        super().__init__(max_size, name, warn_on_discard)
        self._discard_delay = discard_delay
        self._num_items_to_discard = num_items_to_discard
        self._num_discarded_items = 0

    def _get_discard_delay(self) -> float:
        return self._discard_delay

    def _discard_queued_items(self) -> int:
        # Override only to be able to log number of discarded items
        self._num_discarded_items = super()._discard_queued_items()
        return self._num_discarded_items

    def validate_num_discarded_items(self, test_case: TestCase) -> None:
        test_case.assertFalse(self._num_items_to_discard == NUM_ITEMS_TO_DISCARD_NOT_SET, f'{self.buffer_name()} {NUM_ITEMS_TO_DISCARD_NOT_SET_ERROR_MSG}')
        test_case.assertEqual(self._num_items_to_discard, self._num_discarded_items)


class TestMultiProcessBuffer(MultiProcessBuffer[TestVal], NotATestCase):
    _discard_delay: float = copied("_discard_delay")
    _num_items_to_discard: int = copied("_num_items_to_discard")
    _num_discarded_items: int = unmanaged("_num_discarded_items")

    def __init__(self, max_size: int, name: str, *, discard_delay: float = DISCARD_DELAY, warn_on_discard: bool = True, num_items_to_discard: int = NUM_ITEMS_TO_DISCARD_NOT_SET):
        super().__init__(max_size, name, warn_on_discard)
        self._discard_delay = discard_delay
        self._num_items_to_discard = num_items_to_discard
        self._num_discarded_items = 0

    def _get_discard_delay(self) -> float:
        return self._discard_delay

    def _discard_queued_items(self) -> int:
        # Override only to be able to log number of discarded items
        self._num_discarded_items = super()._discard_queued_items()
        return self._num_discarded_items

    def validate_num_discarded_items(self, test_case: TestCase) -> None:
        test_case.assertFalse(self._num_items_to_discard == NUM_ITEMS_TO_DISCARD_NOT_SET, f'{self.buffer_name()} {NUM_ITEMS_TO_DISCARD_NOT_SET_ERROR_MSG}')
        test_case.assertEqual(self._num_items_to_discard, self._num_discarded_items)
