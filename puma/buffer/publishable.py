from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from puma.buffer import Publisher

Type = TypeVar("Type")


class Publishable(Generic[Type], ABC):
    """Interface for the sending end of a Buffer."""

    @abstractmethod
    def publish(self) -> Publisher[Type]:
        """Registers to send items using the Publisher.

        Returns: A Publisher object that is used to push items from the buffer.
        If this method is used in a 'with' statement, the returned Publisher will unpublish itself when it goes out of scope.
        Only one publisher can be obtained. Calling publish again(), without having called unpublish(), will cause an error.
        """
        raise NotImplementedError()

    @abstractmethod
    def unpublish(self, publisher: Publisher[Type]) -> None:
        """Remove the publisher. It is not usually necessary for the owner to use this method, it is called by Publisher when it exits context management.

        Unpublishing more than once causes an error to be logged but is otherwise ignored.
        """
        raise NotImplementedError()

    def buffer_name(self) -> str:
        """Returns the buffer's name."""
        raise NotImplementedError()
