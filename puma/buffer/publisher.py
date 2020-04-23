from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar

from puma.context import Exit_1, Exit_2, Exit_3
from puma.primitives import AutoResetEvent
from puma.timeouts import TIMEOUT_NO_WAIT
from puma.unexpected_situation_action import UnexpectedSituationAction

Type = TypeVar("Type")

DEFAULT_PUBLISH_VALUE_TIMEOUT = TIMEOUT_NO_WAIT
DEFAULT_PUBLISH_COMPLETE_TIMEOUT = 10.0


class Publisher(Generic[Type], ABC):
    """An interface for the object returned by Publishable.publish(). Used to push items to a buffer."""

    @abstractmethod
    def __enter__(self) -> 'Publisher[Type]':
        """Context management. The publication should be context managed; it will unpublish itself when it goes out of scope."""
        raise NotImplementedError()

    @abstractmethod
    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        raise NotImplementedError()

    @abstractmethod
    def publish_value(self, value: Type,
                      timeout: float = DEFAULT_PUBLISH_VALUE_TIMEOUT, on_full_action: UnexpectedSituationAction = UnexpectedSituationAction.RAISE_EXCEPTION) -> None:
        """Accepts the given value and conveys it to the Subscription.

        Parameters:
            value:          Data to be conveyed to the Subscription
            timeout:        Optional time to block if the buffer is full. Defaults to non-blocking.
            on_full_action: Optional action to take if the item cannot be pushed because the buffer is full. If RAISE_EXCEPTION (the default), queue.Full is thrown.

        Raises:
            queue.Full  if the buffer is full and on_full_action is RAISE_EXCEPTION. Note that if the Subscription end of the buffer is connected to a Multicaster,
                        it is not expected to fill up, instead errors will occur in the multicaster if data is not being processed quickly enough.
        """
        raise NotImplementedError()

    @abstractmethod
    def publish_complete(self, error: Optional[BaseException],
                         timeout: float = DEFAULT_PUBLISH_COMPLETE_TIMEOUT, on_full_action: UnexpectedSituationAction = UnexpectedSituationAction.RAISE_EXCEPTION) -> None:
        """Indicates that there will be no more data, optionally including a fatal error reason. This information is conveyed to the Subscription.

        Calling publish() after complete() has been called will raise an exception. Calling publish_complete more than once will also raise an exception.

        Parameters:
            error:          Optional exception to be conveyed to the Subscription.
            timeout:        Optional time to block if the buffer is full. Defaults to blocking for a few seconds.
            on_full_action: Optional action to take if the item cannot be pushed because the buffer is full. If RAISE_EXCEPTION (the default), queue.Full is thrown.

        Raises:
            queue.Full  if the buffer is full and on_full_action is RAISE_EXCEPTION. Note that if the Subscription end of the buffer is connected to a Multicaster,
                        it is not expected to fill up, instead errors will occur in the multicaster if data is not being processed quickly enough.
        """
        raise NotImplementedError()

    @abstractmethod
    def buffer_name(self) -> str:
        """Returns the buffer's name."""
        raise NotImplementedError()

    @abstractmethod
    def invalidate(self) -> None:
        # Called by the buffer when the publication is unpublished. Should not be called by user code.
        raise NotImplementedError()

    @abstractmethod
    def set_subscriber_event(self, subscriber_event: Optional[AutoResetEvent]) -> None:
        # Called by the buffer when the subscriber is subscribed or unsubscribed. Should not be called by user code.
        raise NotImplementedError()
