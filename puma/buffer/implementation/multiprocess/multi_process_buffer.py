import logging
import multiprocessing
import queue
from multiprocessing import synchronize
from typing import Optional, TypeVar

from puma.attribute import factory, python_default, unmanaged
from puma.buffer import Publisher, Subscription
from puma.buffer._queues import _ThreadQueue
from puma.buffer.implementation.managed_queues import ManagedProcessQueue
from puma.buffer.implementation.multiprocess._multi_process_publisher_impl import _MultiProcessPublisherImpl
from puma.buffer.implementation.multiprocess._multi_process_subscription_impl import _MultiProcessSubscriptionImpl
from puma.buffer.internal.buffer_base import BufferBase
from puma.buffer.internal.items.queue_item import QueueItem
from puma.context import Exit_1, Exit_2, Exit_3
from puma.helpers.os import is_windows
from puma.primitives import AutoResetEvent, ConditionType, EventType, ProcessCondition, ProcessEvent, ProcessSafeBool, ProcessSafeInt, SafeBoolType, SafeIntType

Type = TypeVar("Type")

logger = logging.getLogger(__name__)

# Items are deleted when there are no publishers and no subscribers, after a delay of DISCARD_DELAY - see the comment in BufferBase.__init__().
if is_windows():
    DISCARD_DELAY = 15.0  # On Windows, processes are slow to start, so we need to allow time in case another one is being started to make more use of the buffer
else:
    DISCARD_DELAY = 5.0

# It takes time for the underlying queue to transfer items across - especially large items - so we need to allow a timeout when popping items for discarding them.
# This is important - if we don't allow enough time, items are left in the buffer, and the owning process will deadlock on exit.
DISCARD_TIMEOUT = 0.1


class MultiProcessBuffer(BufferBase[Type]):
    """A FIFO buffer that communicates items from one process (Publisher) to another (Observable)."""
    _comms_queue: ManagedProcessQueue[QueueItem] = unmanaged("_comms_queue")
    _emptiness: synchronize.BoundedSemaphore = unmanaged("_emptiness")
    _subscriber_queue: _ThreadQueue = python_default("_subscriber_queue")

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
        logger.debug("Creating multi-process buffer; given name '%s' -> actual name '%s'; size %d", str(name), self._name, max_size)
        if max_size < 1:
            raise RuntimeError(f"{self._name}: Buffer must be created with a size of a least 1")
        self._comms_queue = ManagedProcessQueue(name=self._name)  # no maximum size - fullness is implemented using the emptiness semaphore
        self._emptiness = multiprocessing.BoundedSemaphore(max_size)
        self._subscriber_queue = factory(_ThreadQueue[QueueItem])  # no maximum size - fullness is implemented using the emptiness semaphore

    def __enter__(self) -> 'MultiProcessBuffer[Type]':
        self._comms_queue.__enter__()
        super().__enter__()
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        logger.debug("%s: Multi-process buffer exiting", self._name)
        super().__exit__(exc_type, exc_value, traceback)
        self._comms_queue.__exit__(exc_type, exc_value, traceback)

    def subscribe(self, event: Optional[AutoResetEvent]) -> Subscription[Type]:
        with self._publishers_subscribers.get_lock():
            subscription = super().subscribe(event)
            if event and not self._subscriber_queue.empty():
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
                val = self._comms_queue.get(timeout=DISCARD_TIMEOUT)
            except queue.Empty:
                break
            else:
                count += 1
            self._handle_discarded_item(val)
        while True:
            try:
                val = self._subscriber_queue.get(timeout=DISCARD_TIMEOUT)
            except queue.Empty:
                break
            else:
                count += 1
            self._handle_discarded_item(val)
        return count

    def _empty_test(self) -> bool:
        # Called when there are no publishers and no subscribers, and within _publishers_subscribers.get_lock()
        return self._comms_queue.empty() and self._subscriber_queue.empty()

    def _publisher_factory(self, subscriber_event: Optional[AutoResetEvent]) -> Publisher[Type]:
        return _MultiProcessPublisherImpl(self._comms_queue, self._subscriber_queue, self, self._name, self._emptiness)

    def _subscriber_factory(self, subscriber_event: Optional[AutoResetEvent]) -> Subscription[Type]:
        return _MultiProcessSubscriptionImpl(self._comms_queue, self._subscriber_queue, self, self._name, self._emptiness, subscriber_event)

    def _event_factory(self) -> EventType:
        return ProcessEvent()

    def _condition_factory(self) -> ConditionType:
        return ProcessCondition()

    def _safe_int_factory(self, initial_value: int) -> SafeIntType:
        return ProcessSafeInt(initial_value)

    def _safe_bool_factory(self, initial_value: bool) -> SafeBoolType:
        return ProcessSafeBool(initial_value)
