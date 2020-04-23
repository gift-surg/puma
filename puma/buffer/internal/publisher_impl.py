import logging
import queue
from abc import abstractmethod
from typing import Any, Optional, TypeVar

from puma.buffer import DEFAULT_PUBLISH_COMPLETE_TIMEOUT, DEFAULT_PUBLISH_VALUE_TIMEOUT, Publishable, Publisher
from puma.buffer._queues import _ThreadQueue
from puma.buffer.internal.items.complete_item import CompleteItem
from puma.buffer.internal.items.queue_item import QueueItem
from puma.buffer.internal.items.value_item import ValueItem
from puma.context import Exit_1, Exit_2, Exit_3
from puma.helpers.string import safe_str
from puma.primitives import AutoResetEvent
from puma.timeouts import Timeouts
from puma.unexpected_situation_action import UnexpectedSituationAction, handle_unexpected_situation

Type = TypeVar("Type")

logger = logging.getLogger(__name__)


class PublisherImpl(Publisher[Type]):
    def __init__(self, subscriber_queue: _ThreadQueue, given_publishable: Publishable[Type], name: str):
        self._subscriber_queue = subscriber_queue
        self._given_publishable: Optional[Publishable[Type]] = given_publishable  # Optional because we use None to indicate we have been unpublished
        self._name = name
        self._published_complete: bool = False

    def __enter__(self) -> 'Publisher[Type]':
        logger.debug("%s: Publisher entering context management", self._name)
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        unrolling_after_exception: bool = exc_type is not None
        try:
            logger.debug("%s: Publisher exiting (with error: %s)", self._name, safe_str(exc_type))
            if self._given_publishable:
                self._given_publishable.unpublish(self)  # will call our invalidate() method, setting self._given_publishable to None
        except RuntimeError as ex:
            if unrolling_after_exception:
                logger.warning("%s: Swallowing errors when exiting context management, because already rolling back after an error. Swallowing '%s', already handling '%s'",
                               self._name, safe_str(ex), safe_str(exc_value))
            else:
                raise

    def __getstate__(self) -> Any:
        raise RuntimeError(f"{self._name}: PublisherImpl must not be sent across a process boundary")

    def set_subscriber_event(self, subscriber_event: Optional[AutoResetEvent]) -> None:
        # Overridden in multi-thread implementation
        pass

    def publish_value(self, value: Type,
                      timeout: float = DEFAULT_PUBLISH_VALUE_TIMEOUT, on_full_action: UnexpectedSituationAction = UnexpectedSituationAction.RAISE_EXCEPTION) -> None:
        """Implementation of Publisher.publish_value"""
        logger.debug("%s Publishing value %s, with timeout %s", self._name, safe_str(value), Timeouts.describe(timeout))
        if self._published_complete:
            raise RuntimeError(f"{self._name}: Trying to publish a value after publishing Complete")
        self._publish_item(ValueItem[Type](value), timeout, on_full_action)

    def publish_complete(self, error: Optional[BaseException],
                         timeout: float = DEFAULT_PUBLISH_COMPLETE_TIMEOUT, on_full_action: UnexpectedSituationAction = UnexpectedSituationAction.RAISE_EXCEPTION) -> None:
        """Implementation of Publisher.publish_complete"""
        logger.debug("%s Publishing Complete (with error '%s'), with timeout %s", self._name, safe_str(error), Timeouts.describe(timeout))
        if (error is not None) and (not isinstance(error, Exception)):
            raise TypeError(f"{self._name}: If an error is supplied, it must be an instance of Exception")
        if self._published_complete:
            raise RuntimeError(f"{self._name}: Trying to publish Complete more than once")
        try:
            self._publish_item(CompleteItem(error), timeout, on_full_action)
        except queue.Full:
            raise  # Already logged, in _publish_item
        else:
            self._published_complete = True

    def buffer_name(self) -> str:
        return self._name

    def invalidate(self) -> None:
        logger.debug("%s: publisher invalidate", self._name)
        if not self._given_publishable:
            raise RuntimeError(f"{self._name}: Publication has already been invalidated")
        self._given_publishable = None

    @abstractmethod
    def _publish_item(self, item: QueueItem, timeout: float, on_full_action: UnexpectedSituationAction) -> None:
        # Called by publish_value and publish_complete, puts an item on the queue
        raise NotImplementedError()

    def _handle_buffer_full_exception(self, on_full_action: UnexpectedSituationAction) -> None:
        # Utility method for use by derived classes, to gracefully handle the buffer full condition
        handle_unexpected_situation(on_full_action, f"{self._name}: Buffer full", logger,
                                    exception_factory=lambda s: queue.Full(s))  # if on_full_action=RAISE_EXCEPTION, re-raise queue.Full rather than RuntimeError
