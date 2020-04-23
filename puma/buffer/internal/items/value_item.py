from typing import Generic, TypeVar

from puma.buffer.internal.items.queue_item import QueueItem

Type = TypeVar("Type")


class ValueItem(Generic[Type], QueueItem):
    """An item queued by Publisher.publish_value()"""

    def __init__(self, value: Type) -> None:
        self.value = value

    def __str__(self) -> str:
        return f"ValueItem: {self.value}"
