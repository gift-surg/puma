from abc import ABC
from typing import TypeVar

from puma.buffer import Observable, Publishable
from puma.context import Exit_1, Exit_2, Exit_3

Type = TypeVar("Type", covariant=True)


class Buffer(Publishable[Type], Observable[Type], ABC):
    """Abstract base class for a FIFO buffer supporting Publishable and Observable, which can transport items across threads or processes."""

    def __enter__(self) -> 'Buffer[Type]':
        """Context management. Buffers should be created in context management, before being published to or subscribed to."""
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        pass
