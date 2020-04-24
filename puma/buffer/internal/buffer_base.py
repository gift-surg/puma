import logging
import threading
import time
from abc import abstractmethod
from threading import Thread
from typing import Optional, Set, TypeVar

from puma.attribute import copied, factory, per_scope_value, python_default, unmanaged
from puma.attribute.mixin import ScopedAttributesMixin
from puma.buffer import Buffer, Publisher, Subscription
from puma.buffer.internal.items.complete_item import CompleteItem
from puma.buffer.internal.items.queue_item import QueueItem
from puma.buffer.internal.items.value_item import ValueItem
from puma.context import Exit_1, Exit_2, Exit_3
from puma.helpers.string import safe_str
from puma.primitives import AutoResetEvent, ConditionType, EventType, SafeBoolType, SafeIntType

Type = TypeVar("Type")

logger = logging.getLogger(__name__)


class BufferBase(ScopedAttributesMixin, Buffer[Type]):
    """Abstract base class implementing common methods for buffers that communicate items from one thread or process (Publisher) to another (Observable)."""
    _name: str = copied("_name")
    _publishers: Set[Publisher[Type]] = python_default("_publishers")
    _subscription: Optional[Subscription[Type]] = copied("_subscription")
    _publishers_subscribers: SafeIntType = unmanaged("_publishers_subscribers")
    _on_complete_discarded: SafeBoolType = unmanaged("_on_complete_discarded")
    _subscriber_event: Optional[AutoResetEvent] = python_default("_subscriber_event")

    _discard_after_delay_thread: Optional[Thread] = copied("_discard_after_delay_thread")
    _discard_thread_error: Optional[Exception] = copied("_discard_thread_error")
    _discard_after_delay_condition: ConditionType = unmanaged("_discard_after_delay_condition")
    _cancel_discard_after_delay: SafeBoolType = unmanaged("_cancel_discard_after_delay")
    _discard_after_delay_thread_not_running: EventType = unmanaged("_discard_after_delay_thread_not_running")
    _warn_on_discard: Optional[bool] = copied("_warn_on_discard")

    def __init__(self, name: str, warn_on_discard: Optional[bool] = True) -> None:
        """Constructor.

        name: Name for logging.
        warn_on_discard: Whether to log a warning upon discarding items from buffer.
        """
        super().__init__()
        if not name:
            raise RuntimeError(f"A name must be supplied")
        self._name = name
        self._publishers = factory(set)  # The current publishers. In the multi-process case this remains empty at the Observable end.
        self._subscription = per_scope_value(None)  # The current subscription, if any. In the multi-process case this remains None at the Publishable end.
        self._publishers_subscribers = self._safe_int_factory(0)  # Count of the number of publishers and subscribers. Shared between the Publisher and Observable ends.
        self._on_complete_discarded = self._safe_bool_factory(False)  # Whether on_complete was discarded by the discard thread. Shared between the Publisher and Observable ends.
        self._subscriber_event = per_scope_value(None)  # Event given when subscribing. In the multi-process case this is only at the Observable end.

        # Implementation of the "discard thread". This is launched when the last publisher or subscriber disconnects, so the buffer has no "users", and the buffer is not empty.
        # After a few seconds, it discards the items that were in the buffer.
        # If another publisher or subscriber connects in the meantime, the thread is cancelled.
        # In the multiprocessing case, the thread could run at either the publisher or subscriber end of the buffer, depending which was the last to be used. There will
        # only ever be one discard thread running (or none) - never two.
        # The reason for this thread is that a process cannot end if it has put items onto a multi-process buffer, and that buffer is not empty - the garbage
        # collection thread hangs.
        # See https://stackoverflow.com/questions/31665328/python-3-multiprocessing-queue-deadlock-when-calling-join-before-the-queue-is-em.
        self._discard_after_delay_thread = per_scope_value(None)  # The discard thread (in the multiprocessing case, the thread at "this end" of the buffer).
        self._discard_thread_error = None  # Error from discard thread. Separate instances at each end of the multi-process buffer.
        self._discard_after_delay_condition = self._condition_factory()  # Condition used to wake the discard thread. Shared between the Publisher and Observable ends.
        self._cancel_discard_after_delay = self._safe_bool_factory(False)  # Instructs the discard thread to cancel. Shared between the Publisher and Observable ends.
        self._discard_after_delay_thread_not_running = self._event_factory()  # Event cleared when the discard thread is running. Shared between the Publisher and Observable ends.
        self._discard_after_delay_thread_not_running.set()
        self._warn_on_discard = warn_on_discard

    def __enter__(self) -> 'BufferBase[Type]':
        logger.debug("%s: Entering context management", self._name)
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        with self._publishers_subscribers.get_lock():
            self._cancel_discard_after_delay_threads()
            unrolling_after_exception: bool = exc_type is not None
            logger.debug("%s: Exiting context management; unrolling after exception: %s", self._name, str(unrolling_after_exception))
            if self._publishers:
                logger.warning("%s: Buffer being destroyed while still published to", self._name)
                for publisher in self._publishers:
                    publisher.invalidate()
                self._publishers.clear()
            if self._subscription:
                logger.warning("%s: Buffer being destroyed while still subscribed to", self._name)
                self._subscription.invalidate()
                self._subscription = None
            self._publishers_subscribers.value = 0
            self._discard_queued_items_and_warn_if_any(f"{self._name} Exit")
            self._check_for_discard_thread_error()
        logger.debug("%s: exited context management", self._name)

    def publish(self) -> Publisher[Type]:
        logger.debug("%s: Being published to", self._name)
        with self._publishers_subscribers.get_lock():
            self._check_for_discard_thread_error()
            self._cancel_discard_after_delay_threads()
            publisher = self._publisher_factory(self._subscriber_event)
            self._publishers.add(publisher)
            self._publishers_subscribers.value += 1
            logger.debug("%s: Finished being published to (publishers count now %d)", self._name, len(self._publishers))
            return publisher

    def unpublish(self, publisher: Publisher[Type]) -> None:
        if publisher is None:
            raise ValueError(f"{self._name}: Unpublish: publisher must not be None")
        logger.debug("%s: Being unpublished from", self._name)
        with self._publishers_subscribers.get_lock():
            self._check_for_discard_thread_error()
            if publisher not in self._publishers:
                logger.warning("%s: Ignoring buffer unpublish, not published", self._name)
                return
            publisher.invalidate()
            self._publishers.remove(publisher)
            self._publishers_subscribers.value -= 1
            self._launch_discard_after_delay_thread_if_no_publishers_and_no_subscriber_and_buffer_not_empty()
        logger.debug("%s: finished being unpublished from", self._name)

    def subscribe(self, event: Optional[AutoResetEvent]) -> Subscription[Type]:
        logger.debug("%s: Being subscribed to", self._name)
        if (event is not None) and (not isinstance(event, AutoResetEvent)):
            raise TypeError("If an event is supplied, it must be an AutoResetEvent")
        with self._publishers_subscribers.get_lock():
            self._check_for_discard_thread_error()
            self._cancel_discard_after_delay_threads()
            if self._subscription:
                raise RuntimeError(f"{self._name}: Can't subscribe, already subscribed to")
            self._subscriber_event = event
            self._subscription = self._subscriber_factory(event)
            self._publishers_subscribers.value += 1
            for publisher in self._publishers:
                publisher.set_subscriber_event(event)
            if self._on_complete_discarded.value:
                logger.debug("%s: Re-pushing the previously discarded on_complete", self._name)
                with self.publish() as completion_publisher:
                    completion_publisher.publish_complete(None)
                self._on_complete_discarded.value = False
            logger.debug("%s: Finished being subscribed to", self._name)
            return self._subscription

    def unsubscribe(self) -> None:
        logger.debug("%s: Being unsubscribed from", self._name)
        with self._publishers_subscribers.get_lock():
            self._check_for_discard_thread_error()
            if not self._subscription:
                logger.warning("%s: Ignoring buffer unsubscribe, not subscribed", self._name)
                return
            self._subscription.invalidate()
            self._subscription = None
            self._publishers_subscribers.value -= 1
            self._subscriber_event = None
            for publisher in self._publishers:
                publisher.set_subscriber_event(None)
            logger.debug("%s: finished being unsubscribed from", self._name)
            self._launch_discard_after_delay_thread_if_no_publishers_and_no_subscriber_and_buffer_not_empty()

    def buffer_name(self) -> str:
        """Returns the buffer's name."""
        return self._name

    def _launch_discard_after_delay_thread_if_no_publishers_and_no_subscriber_and_buffer_not_empty(self) -> None:
        # Must be called within _publishers_subscribers.get_lock()
        if threading.current_thread() == self._discard_after_delay_thread:
            return
        if not self._discard_after_delay_thread_not_running.is_set():
            logger.debug("%s: Not starting discard thread: already running", self._name)
            return
        if self._publishers_subscribers.value != 0:
            # TODO problematic line that causes slow Windows tests to run indefinitely (despite succeeding):
            # logger.debug("%s: Not starting discard thread: buffer still has publisher or subscriber", self._name)
            return
        if self._empty_test():
            logger.debug("%s: Not starting discard thread: buffer is empty", self._name)
            return
        logger.debug("%s: Starting discard thread", self._name)
        self._discard_after_delay_thread_not_running.clear()
        self._cancel_discard_after_delay.value = False
        self._discard_after_delay_thread = Thread(name='discard_after_delay', target=self._discard_after_delay_thread_run)
        self._discard_after_delay_thread.start()
        logger.debug("%s: Started discard thread", self._name)

    def _cancel_discard_after_delay_threads(self) -> None:
        # Must be called within _publishers_subscribers.get_lock()
        if threading.current_thread() == self._discard_after_delay_thread:
            return
        if not self._discard_after_delay_thread_not_running.is_set():
            logger.debug("%s: Cancelling discard thread", self._name)
            with self._discard_after_delay_condition:
                self._cancel_discard_after_delay.value = True
                self._discard_after_delay_condition.notify_all()
            self._discard_after_delay_thread_not_running.wait()
            if self._discard_after_delay_thread:
                self._discard_after_delay_thread.join()
            logger.debug("%s: Cancelled discard thread", self._name)

    def _discard_after_delay_thread_run(self) -> None:
        try:
            self._discard_after_delay_thread_run_impl()
        except Exception as ex:
            self._discard_thread_error = ex
        finally:
            self._discard_after_delay_thread_not_running.set()
        logger.debug("%s Discard thread: Ending", self._name)

    def _discard_after_delay_thread_run_impl(self) -> None:
        with self._discard_after_delay_condition:
            delay = self._get_discard_delay()
            logger.debug("%s Discard thread: Starting, will discard items in %f seconds if not cancelled", self._name, delay)
            end_at = time.perf_counter() + delay
            while True:
                if self._cancel_discard_after_delay.value:
                    logger.debug("%s Discard thread: Cancelled", self._name)
                    return
                remaining = end_at - time.perf_counter()
                if remaining <= 0:
                    break
                self._discard_after_delay_condition.wait(remaining)
            with self._publishers_subscribers.get_lock():
                if self._publishers_subscribers.value == 0:
                    logger.debug("%s Discard thread: Not cancelled, about to discard items", self._name)
                    self._discard_queued_items_and_warn_if_any(f"{self._name} Discard thread")

    def _discard_queued_items_and_warn_if_any(self, source: str) -> None:
        num_discarded = self._discard_queued_items()
        if self._warn_on_discard:
            if num_discarded > 0:
                logger.warning("%s: Discarded %d items from the buffer", source, num_discarded)
            else:
                logger.debug("%s: No items discarded", source)

    @abstractmethod
    def _get_discard_delay(self) -> float:
        # Returns the time after which we assume no more items will be pushed to the buffer and we can clean up
        raise NotImplementedError()

    @abstractmethod
    def _discard_queued_items(self) -> int:
        # Called when there are no publishers and no subscribers, and within _publishers_subscribers.get_lock()
        # Calls _handle_discarded_item() on each item
        # Returns the number of items discarded
        raise NotImplementedError()

    @abstractmethod
    def _empty_test(self) -> bool:
        # Called when there are no publishers and no subscribers, and within _publishers_subscribers.get_lock().
        # Returns true if the buffer is empty.
        raise NotImplementedError()

    def _handle_discarded_item(self, item: QueueItem) -> None:
        if isinstance(item, ValueItem):
            logger.debug("%s: Discarding item %s", self._name, safe_str(item.value))
        elif isinstance(item, CompleteItem):
            err: Optional[BaseException] = item.get_error()
            if err:
                logger.debug("%s: Raising error received when discarding items: (%s)", self._name, safe_str(err))
                raise err
            else:
                logger.debug("%s: Discarding on_complete (None) - will be re-sent if the buffer is re-subscribed", self._name)
                self._on_complete_discarded.value = True
        else:
            raise ValueError(f"{self._name}: Invalid QueueItem received: {safe_str(item)}")

    def _check_for_discard_thread_error(self) -> None:
        if self._discard_thread_error:
            raise self._discard_thread_error

    @abstractmethod
    def _publisher_factory(self, subscriber_event: Optional[AutoResetEvent]) -> Publisher[Type]:
        raise NotImplementedError()

    @abstractmethod
    def _subscriber_factory(self, subscriber_event: Optional[AutoResetEvent]) -> Subscription[Type]:
        raise NotImplementedError()

    @abstractmethod
    def _event_factory(self) -> EventType:
        raise NotImplementedError()

    @abstractmethod
    def _condition_factory(self) -> ConditionType:
        raise NotImplementedError()

    @abstractmethod
    def _safe_int_factory(self, initial_value: int) -> SafeIntType:
        raise NotImplementedError()

    @abstractmethod
    def _safe_bool_factory(self, initial_value: bool) -> SafeBoolType:
        raise NotImplementedError()
