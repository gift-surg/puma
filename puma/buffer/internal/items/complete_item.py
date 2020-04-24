from typing import Optional

from puma.buffer import TraceableException
from puma.buffer.internal.items.queue_item import QueueItem


class CompleteItem(QueueItem):
    """An item queued to represent on_complete, including an optional fatal error indication"""

    def __init__(self, error: Optional[BaseException]) -> None:
        self._error = TraceableException(error) if error else None

    def get_error(self) -> Optional[BaseException]:
        return self._error.get_error() if self._error else None

    def __str__(self) -> str:
        if self._error:
            return f"CompleteItem (with error '{self._error}')"
        else:
            return "CompleteItem (no error)"
