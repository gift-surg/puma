import logging
import queue
from multiprocessing import synchronize
from threading import Thread
from typing import NoReturn, Optional, TypeVar, Union

from puma.buffer import Observable, OnComplete, OnValue, Subscriber
from puma.buffer._queues import _ThreadQueue
from puma.buffer.implementation.managed_queues import ManagedProcessQueue
from puma.buffer.implementation.multiprocess._special_queue_items import _HiddenStopQueueItem
from puma.buffer.internal.items.queue_item import QueueItem
from puma.buffer.internal.subscription_impl import SubscriptionImpl
from puma.helpers.string import safe_str
from puma.primitives import AutoResetEvent

Type = TypeVar("Type")

logger = logging.getLogger(__name__)


class _MultiProcessSubscriptionImpl(SubscriptionImpl):
    def __init__(self, comms_queue: ManagedProcessQueue[QueueItem],
                 subscriber_queue: _ThreadQueue[QueueItem],
                 given_observable: Observable[Type],
                 name: str,
                 emptiness: synchronize.BoundedSemaphore,
                 event: Optional[AutoResetEvent]):
        super().__init__(subscriber_queue, given_observable, name)
        self._comms_queue: ManagedProcessQueue[QueueItem] = comms_queue
        self._subscriber_queue: _ThreadQueue[QueueItem] = subscriber_queue
        self._emptiness = emptiness
        self._subscription_event = event
        self._invalidated = False
        self._thread_error: Optional[Exception] = None
        self._deal_with_existing_queue_items()
        logger.debug("%s: Launching subscription thread", self._name)
        self._subscription_thread = Thread(name="Subscription Thread for " + self._name, target=self._subscription_thread_run)
        self._subscription_thread.start()

    def __getstate__(self) -> NoReturn:
        raise RuntimeError(f"{self._name}: _MultiProcessSubscriptionImpl must not be sent across a process boundary")

    def _subscription_thread_run(self) -> None:
        # This method runs in a separate thread, self._subscription_thread
        logger.debug("%s: Subscription thread running", self._name)
        try:
            while not self._invalidated:
                logger.debug("%s: Subscription thread waiting for data on comms queue", self._name)
                val = self._comms_queue.get(block=True, timeout=None)
                logger.debug("%s: Received %s from comms queue", self._name, safe_str(val))
                self._transfer_item(val)
        except Exception as ex:
            logger.error("%s: Error in subscription thread: %s", self._name, safe_str(ex), exc_info=True)
            self._thread_error = ex
        logger.debug("Subscription thread stopped")

    def _check_for_subscription_thread_errors(self) -> None:
        if self._thread_error:
            raise self._thread_error

    def _stop_subscription_thread(self) -> None:
        if self._subscription_thread and self._subscription_thread.is_alive():
            logger.debug("%s: Sending stop instruction to the subscription thread", self._name)
            self._comms_queue.put_nowait(_HiddenStopQueueItem())
            self._subscription_thread.join(30.0)
            if self._subscription_thread.is_alive():
                raise RuntimeError(f"{self._name}: Failed to stop the subscription thread")

    def _deal_with_existing_queue_items(self) -> None:
        if not self._subscriber_queue.empty():
            logger.debug("%s: Items already waiting in subscriber queue, notifying event", self._name)
            self._set_event()

        logger.debug("%s: Transferring waiting items from comms queue to subscriber queue", self._name)
        while not self._invalidated:
            try:
                val = self._comms_queue.get_nowait()
                logger.debug("%s: Received %s from comms queue", self._name, safe_str(val))
                self._transfer_item(val)
            except queue.Empty:
                break
        logger.debug("%s: Done transferring waiting items from comms queue to subscriber queue", self._name)

    def _transfer_item(self, val: QueueItem) -> None:
        if isinstance(val, _HiddenStopQueueItem):
            self._invalidated = True
        else:
            self._subscriber_queue.put_nowait(val)
            self._set_event()

    def _set_event(self) -> None:
        if self._subscription_event:
            self._subscription_event.set()

    def invalidate(self) -> None:
        logger.debug("%s: Invalidating subscription - stopping subscription thread", self._name)
        self._stop_subscription_thread()
        self._check_for_subscription_thread_errors()
        super().invalidate()

    def call_events(self, on_value_or_subscriber: Union[OnValue[Type], Subscriber[Type]], on_complete: Optional[OnComplete] = None) -> None:
        logger.debug("%s: call_events", self._name)
        self._validate_call_events_params(on_value_or_subscriber, on_complete)

        try:
            super()._call_events_impl(on_value_or_subscriber, on_complete)
        except queue.Empty as e:
            logger.debug("%s: call_events: queue empty", self._name)
            raise queue.Empty(self._name) from e
        else:
            self._emptiness.release()
        finally:
            self._check_for_subscription_thread_errors()
