import logging
from multiprocessing import synchronize
from typing import TypeVar

from puma.buffer import Publishable
from puma.buffer._queues import _ThreadQueue
from puma.buffer.implementation.managed_queues import ManagedProcessQueue
from puma.buffer.internal.items.queue_item import QueueItem
from puma.buffer.internal.publisher_impl import PublisherImpl
from puma.timeouts import Timeouts
from puma.unexpected_situation_action import UnexpectedSituationAction

Type = TypeVar("Type")

logger = logging.getLogger(__name__)


class _MultiProcessPublisherImpl(PublisherImpl[Type]):
    def __init__(self,
                 comms_queue: ManagedProcessQueue,
                 subscriber_queue: _ThreadQueue,
                 given_publishable: Publishable[Type],
                 name: str,
                 emptiness: synchronize.BoundedSemaphore) -> None:
        super().__init__(subscriber_queue, given_publishable, name)
        self._comms_queue = comms_queue
        self._emptiness = emptiness

    def _publish_item(self, item: QueueItem, timeout: float, on_full_action: UnexpectedSituationAction = UnexpectedSituationAction.RAISE_EXCEPTION) -> None:
        logger.debug("%s: publishing %s", self._name, str(item))
        if not self._emptiness.acquire(block=Timeouts.is_blocking(timeout), timeout=Timeouts.timeout_for_queue(timeout)):
            self._handle_buffer_full_exception(on_full_action)
            return
        self._comms_queue.put_nowait(item)
        logger.debug("%s: published %s", self._name, str(item))
