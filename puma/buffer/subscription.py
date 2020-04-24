import typing
from abc import abstractmethod
from typing import Callable, Generic, Optional, TypeVar, Union

from puma.buffer import Subscriber
from puma.context import Exit_1, Exit_2, Exit_3

Type = TypeVar("Type")

OnValue = Callable[[Type], None]
OnComplete = Callable[[Optional[BaseException]], None]


class Subscription(Generic[Type]):
    """An interface for the object returned by Observer.subscribe(). Used to pop items from a buffer."""

    @abstractmethod
    def __enter__(self) -> 'Subscription[Type]':
        """Context management. The subscription should be context managed; it will unsubscribe itself when it goes out of scope."""
        raise NotImplementedError()

    @abstractmethod
    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        raise NotImplementedError()

    @typing.overload
    @abstractmethod
    def call_events(self, on_value_or_subscriber: Subscriber[Type]) -> None:
        """Non-blocking call that polls for items on the buffer.

        If an item is available to pop from the buffer then it is popped and the appropriate callback is immediately called, from within this call.
        This call returns once the callback returns.
        If the buffer is empty then the method raises queue.Empty.

        Parameters:
            on_value_or_subscriber: An object implementing the Subscriber interface.
        Raises:
            queue.Empty if the buffer was empty
        """
        ...

    @typing.overload  # noqa: F811
    @abstractmethod
    def call_events(self, on_value_or_subscriber: OnValue[Type], on_complete: Optional[OnComplete] = None) -> None:
        """Non-blocking call that polls for items on the buffer.

        If an item is available to pop from the buffer then it is popped and the appropriate callback is immediately called, from within this call.
        This call returns once the callback returns.
        If the buffer is empty then the method raises queue.Empty.

        For notes on implementing on_value and on_complete, see the Subscriber interface.

        Parameters:
            on_value_or_subscriber: A method that is called by the Subscriber to deliver a value published with the Publisher's publish() method.
            on_complete: A method that is called by the Subscriber to indicate that the Publisher's complete() method was called, optionally including a fatal error.
        Raises:
            queue.Empty if the buffer was empty
        """
        ...

    @abstractmethod  # noqa: F811
    def call_events(self, on_value_or_subscriber: Union[OnValue[Type], Subscriber[Type]], on_complete: Optional[OnComplete] = None) -> None:
        # Implementation signature of overloaded method, see above definitions.
        raise NotImplementedError()

    def buffer_name(self) -> str:
        """Returns the buffer's name."""
        raise NotImplementedError()

    @abstractmethod
    def invalidate(self) -> None:
        # Called by the buffer when the subscription is unsubscribed. Should not be called by user code.
        raise NotImplementedError()
