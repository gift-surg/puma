import logging
import queue
from typing import NoReturn, Optional, TypeVar

from puma.attribute import copied
from puma.attribute.mixin import ScopedAttributeState
from puma.buffer import Publisher, Subscription
from puma.buffer.implementation.managed_queues import ManagedThreadQueue
from puma.buffer.implementation.multithread._multi_thread_publisher_impl import _MultiThreadPublisherImpl
from puma.buffer.internal.buffer_base import BufferBase
from puma.buffer.internal.items.queue_item import QueueItem
from puma.buffer.internal.subscription_impl import SubscriptionImpl
from puma.context import Exit_1, Exit_2, Exit_3
from puma.primitives import AutoResetEvent, ConditionType, EventType, SafeBoolType, SafeIntType, ThreadCondition, ThreadEvent, ThreadSafeBool, ThreadSafeInt

MULTI_THREAD_BUFFER_ACROSS_PROCESSES_WARNING = "MultiThreadBuffers cannot be shared across processes - use a MultiProcessBuffer instead"

Type = TypeVar("Type")

logger = logging.getLogger(__name__)

# Items are deleted when there are no publishers and no subscribers, after a delay of DISCARD_DELAY - see the comment in BufferBase.__init__().
DISCARD_DELAY = 5.0


class MultiThreadBuffer(BufferBase[Type]):
    """A FIFO buffer that communicates items from one thread (Publishable) to another (Observable)."""
    _queue: ManagedThreadQueue[QueueItem] = copied("_queue")

    def __init__(self,
                 max_size: int,
                 name: str,
                 warn_on_discard: Optional[bool] = True) -> None:
        """Constructor.

        max_size: Maximum number of items that the buffer can contain.
        name: Name for logging.
        warn_on_discard: see BufferBase.__init__
        """
        super().__init__(name, warn_on_discard)
        logger.debug("Creating multi-threaded buffer; given name '%s' -> actual name '%s'; size %d", str(name), self._name, max_size)
        if max_size < 1:
            raise RuntimeError(f"{self._name}: Buffer must be created with a size of a least 1")
        self._queue = ManagedThreadQueue(max_size, name)

    def __enter__(self) -> 'MultiThreadBuffer[Type]':
        self._queue.__enter__()
        super().__enter__()
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        logger.debug("%s: Multi-threaded buffer exiting", self._name)
        super().__exit__(exc_type, exc_value, traceback)
        self._queue.__exit__(exc_type, exc_value, traceback)

    def __getstate__(self) -> NoReturn:
        logger.warning(MULTI_THREAD_BUFFER_ACROSS_PROCESSES_WARNING)
        raise RuntimeError(MULTI_THREAD_BUFFER_ACROSS_PROCESSES_WARNING)

    def __setstate__(self, state: ScopedAttributeState) -> None:
        raise RuntimeError(MULTI_THREAD_BUFFER_ACROSS_PROCESSES_WARNING)

    def subscribe(self, event: Optional[AutoResetEvent]) -> Subscription[Type]:
        with self._publishers_subscribers.get_lock():
            subscription = super().subscribe(event)
            if event and not self._queue.empty():
                logger.debug("%s: Raising event to trigger processing of items already in the queue", self._name)
                event.set()
            return subscription

    def _get_discard_delay(self) -> float:
        return DISCARD_DELAY

    def _discard_queued_items(self) -> int:
        # Called when there are no publishers and no subscribers, and within _publishers_subscribers.get_lock()
        count = 0
        while True:
            try:
                self._handle_discarded_item(self._queue.get_nowait())
                count += 1
            except queue.Empty:
                break
        return count

    def _empty_test(self) -> bool:
        # Called when there are no publishers and no subscribers, and within _publishers_subscribers.get_lock()
        return self._queue.empty()

    def _publisher_factory(self, subscriber_event: Optional[AutoResetEvent]) -> Publisher[Type]:
        return _MultiThreadPublisherImpl(self._queue, self, self._name, subscriber_event)

    def _subscriber_factory(self, subscriber_event: Optional[AutoResetEvent]) -> Subscription[Type]:
        return SubscriptionImpl(self._queue, self, self._name)

    def _event_factory(self) -> EventType:
        return ThreadEvent()

    def _condition_factory(self) -> ConditionType:
        return ThreadCondition()

    def _safe_int_factory(self, initial_value: int) -> SafeIntType:
        return ThreadSafeInt(initial_value)

    def _safe_bool_factory(self, initial_value: bool) -> SafeBoolType:
        return ThreadSafeBool(initial_value)
