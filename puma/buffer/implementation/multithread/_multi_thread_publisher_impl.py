import logging
import queue
from typing import Optional, TypeVar

from puma.buffer import Publishable
from puma.buffer._queues import _ThreadQueue
from puma.buffer.internal.items.queue_item import QueueItem
from puma.buffer.internal.publisher_impl import PublisherImpl
from puma.primitives import AutoResetEvent, ThreadRLock
from puma.timeouts import Timeouts
from puma.unexpected_situation_action import UnexpectedSituationAction

Type = TypeVar("Type")

logger = logging.getLogger(__name__)


class _MultiThreadPublisherImpl(PublisherImpl[Type]):
    def __init__(self, subscriber_queue: _ThreadQueue, given_publishable: Publishable[Type], name: str, subscriber_event: Optional[AutoResetEvent]) -> None:
        super().__init__(subscriber_queue, given_publishable, name)
        self._subscriber_event = subscriber_event
        self._subscriber_event_lock = ThreadRLock()

    def _publish_item(self, item: QueueItem, timeout: float, on_full_action: UnexpectedSituationAction) -> None:
        with self._subscriber_event_lock:
            event = self._subscriber_event
        try:
            if event is not None:
                logger.debug("%s: Publishing Item '%s'", self._name, str(item))
                self._queue_item(item, timeout)
                event.set()
            else:
                logger.debug("%s: Publishing Item '%s', no subscription", self._name, str(item))
                self._queue_item(item, timeout)
        except queue.Full:
            self._handle_buffer_full_exception(on_full_action)
        else:
            logger.debug("%s: Published item", self._name)

    def set_subscriber_event(self, subscriber_event: Optional[AutoResetEvent]) -> None:
        with self._subscriber_event_lock:
            self._subscriber_event = subscriber_event

    def _queue_item(self, item: QueueItem, timeout: float) -> None:
        self._subscriber_queue.put(item, block=Timeouts.is_blocking(timeout), timeout=Timeouts.timeout_for_queue(timeout))
