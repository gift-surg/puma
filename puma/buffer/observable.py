from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar

from puma.buffer import Subscription
from puma.primitives import AutoResetEvent

Type = TypeVar("Type")


class Observable(Generic[Type], ABC):
    """Interface for the receiving end of a Buffer."""

    @abstractmethod
    def subscribe(self, event: Optional[AutoResetEvent]) -> Subscription[Type]:
        """Registers to receive items from the Observable.

        Arguments:
            event:  Pass an AutoResetEvent, which will be notified when an item is available in the buffer. Having been notified, call call_events in a loop
                    until it returns False.
                    Alternatively, pass None if the subscriber will poll for items, rather than being woken to respond to them.
        Returns: A Subscription object that is used to pop items from the buffer.
        If this method is used in a 'with' statement, the returned Subscription will unsubscribe itself when it goes out of scope.
        Only one subscription can be obtained. Calling subscribe again(), without having called unsubscribe(), will cause an error.
        """
        raise NotImplementedError()

    @abstractmethod
    def unsubscribe(self) -> None:
        """Unsubscribe the subscriber. It is not usually necessary for the owner to use this method, it is called by Subscription when it exits context management.

        Unsubscribing more than once causes an error to be logged but is otherwise ignored.
        """
        raise NotImplementedError()

    def buffer_name(self) -> str:
        """Returns the buffer's name."""
        raise NotImplementedError()
