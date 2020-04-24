from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar

Type = TypeVar("Type")


class Subscriber(Generic[Type], ABC):
    """An interface containing the three callbacks required when using a Subscription.

    Implementing this interface is one way to use Subscription.call_events.
    """

    @abstractmethod
    def on_value(self, value: Type) -> None:
        """Called by the Subscription to deliver a value published with the Publisher's publish_value() method.

        User code should raise an exception if it cannot handle the value (for example, if a buffer is full).
        """
        raise NotImplementedError()

    @abstractmethod
    def on_complete(self, error: Optional[BaseException]) -> None:
        """Called by the Subscription to indicate that the Publisher's publish_complete() method was called, optionally including a fatal error.

        User code should raise an error if it cannot service the call (for example, if a buffer is full).
        It is fine to raise some new error (such as queue.Full), you do not have to re-raise the error you are given, if there is one.
        """
        raise NotImplementedError()
