from typing import Generic, Optional, TypeVar

from puma.buffer import OnComplete, OnValue, Subscriber

S = TypeVar("S")


class DefaultSubscriber(Generic[S], Subscriber[S]):
    """Converts an on_value method and / or an on_complete method into a Subscriber"""

    def __init__(self, on_value: OnValue[S], on_complete: Optional[OnComplete] = None):
        self._on_value = on_value
        self._on_complete = on_complete

    def on_value(self, value: S) -> None:
        self._on_value(value)

    def on_complete(self, error: Optional[BaseException]) -> None:
        if self._on_complete:
            self._on_complete(error)
        elif error:
            raise error
