from puma.buffer.internal.items.queue_item import QueueItem


class _HiddenStopQueueItem(QueueItem):
    def __str__(self) -> str:
        return "_HiddenStopQueueItem"
