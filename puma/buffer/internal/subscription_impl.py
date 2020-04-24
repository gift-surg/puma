import logging
import queue
from typing import Optional, TypeVar, Union

from puma.buffer import Observable, OnComplete, OnValue, Subscriber, Subscription
from puma.buffer._queues import _ThreadQueue
from puma.buffer.internal.items.complete_item import CompleteItem
from puma.buffer.internal.items.queue_item import QueueItem
from puma.buffer.internal.items.value_item import ValueItem
from puma.context import Exit_1, Exit_2, Exit_3
from puma.helpers.string import safe_str

Type = TypeVar("Type")

logger = logging.getLogger(__name__)


class SubscriptionImpl(Subscription[Type]):
    """Implementation of the Subscription interface. Objects of this type are returned from Observable.subscribe(). They unsubscribe themselves when exiting context management."""

    def __init__(self, given_queue: _ThreadQueue[QueueItem], given_observable: Observable[Type], name: str) -> None:
        self._name = name
        self._queue = given_queue
        self._given_observable: Optional[Observable[Type]] = given_observable  # Optional because we use None to indicate we have been unsubscribed

    def __enter__(self) -> 'Subscription[Type]':
        logger.debug("%s: Subscription entering context management", self._name)
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        unrolling_after_exception: bool = exc_type is not None
        try:
            logger.debug("%s: Subscription exiting context management (with error: %s)", self._name, safe_str(exc_type))
            if self._given_observable:
                self._given_observable.unsubscribe()  # will call our invalidate() method, setting self._given_observable to None
        except RuntimeError as ex:
            if unrolling_after_exception:
                logger.warning("%s: Swallowing errors when exiting context management, because already rolling back after an error. Swallowing '%s', already handling '%s'",
                               self._name, safe_str(ex), safe_str(exc_value))
            else:
                raise

    def call_events(self, on_value_or_subscriber: Union[OnValue[Type], Subscriber[Type]], on_complete: Optional[OnComplete] = None) -> None:
        logger.debug("%s: call_events", self._name)
        self._validate_call_events_params(on_value_or_subscriber, on_complete)
        self._call_events_impl(on_value_or_subscriber, on_complete)

    def buffer_name(self) -> str:
        return self._name

    def _call_events_impl(self, on_value_or_subscriber: Union[OnValue[Type], Subscriber[Type]], on_complete: Optional[OnComplete] = None) -> None:
        if not self._given_observable:
            raise RuntimeError(f"{self._name}: Subscription has been unsubscribed")
        try:
            logger.debug("%s: Polling queue", self._name)
            item = self._queue.get(block=False)
            logger.debug("%s: Popped %s from queue", self._name, str(item))
        except queue.Empty:
            logger.debug("%s: Queue is empty", self._name)
            raise
        logger.debug("%s: Calling out to callbacks with %s", self._name, str(item))
        self._handle_item(item, on_value_or_subscriber, on_complete)

    def invalidate(self) -> None:
        logger.debug("%s: subscription invalidate", self._name)
        if not self._given_observable:
            raise RuntimeError(f"{self._name}: Subscription has already been invalidated")
        self._given_observable = None

    def _validate_call_events_params(self, on_value_or_subscriber: Union[OnValue[Type], Subscriber[Type]], on_complete: Optional[OnComplete]) -> None:
        if on_value_or_subscriber is None:
            raise TypeError(f"{self._name}: on_value_or_subscriber must not be None")
        elif isinstance(on_value_or_subscriber, Subscriber):
            if on_complete:
                raise ValueError(f"{self._name}: on_complete may only be provided if on_value_or_subscriber is a callback function, not a Subscriber")
        elif not callable(on_value_or_subscriber):
            raise TypeError(f"{self._name}: on_value_or_subscriber is not of the correct type")

    def _handle_item(self, item: QueueItem, on_value_or_subscriber: Union[OnValue[Type], Subscriber[Type]], on_complete: Optional[OnComplete]) -> None:
        if isinstance(on_value_or_subscriber, Subscriber):
            subscriber = on_value_or_subscriber
            self._handle_item_callbacks(item, subscriber.on_value, subscriber.on_complete)
        else:
            on_value = on_value_or_subscriber
            self._handle_item_callbacks(item, on_value, on_complete)

    def _handle_item_callbacks(self, item: QueueItem, on_value: OnValue[Type], on_complete: Optional[OnComplete]) -> None:
        if isinstance(item, ValueItem):
            logger.debug("%s: popped value with value '%s', calling on_value", self._name, str(item.value))
            on_value(item.value)
        elif isinstance(item, CompleteItem):
            error: Optional[BaseException] = item.get_error()
            logger.debug("%s: popped complete, with error '%s'; on_complete method given: %s", self._name, safe_str(error), str(bool(on_complete)))
            if on_complete:
                on_complete(error)
        else:
            raise ValueError(f"{self._name}: Invalid QueueItem received: {safe_str(item)}")
