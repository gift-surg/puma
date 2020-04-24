import logging
import queue
from typing import Dict, Optional, TypeVar

from puma.buffer import Publisher, Subscriber
from puma.helpers.string import safe_str
from puma.primitives import ThreadLock
from puma.timeouts import TIMEOUT_NO_WAIT
from puma.unexpected_situation_action import UnexpectedSituationAction, handle_unexpected_situation

Type = TypeVar("Type")

logger = logging.getLogger(__name__)


class _MulticasterSubscriber(Subscriber[Type]):
    """A subscriber used by the multicaster. Copies items to its subscribing buffers.

    If there are no subscribers then received items are discarded.
    The behaviour if a subscribing buffer is full depends on the option given when subscribing that buffer.
    """

    def __init__(self, name: str) -> None:
        super().__init__()
        self._name = name
        self._publishers: Dict[Publisher[Type], UnexpectedSituationAction] = dict()
        self._publishers_lock: ThreadLock = ThreadLock()

    def subscribe(self, publisher: Publisher[Type], on_full_action: UnexpectedSituationAction) -> None:
        # For comments see Multicaster.subscribe()
        if not publisher:
            raise ValueError("Publisher must be supplied")
        if not on_full_action:
            raise ValueError("on_full_action must be supplied")
        with self._publishers_lock:
            if publisher in self._publishers:
                raise RuntimeError("Publisher is already subscribed")
            logger.debug("%s: Subscribing buffer '%s'", self._name, publisher.buffer_name())
            self._publishers[publisher] = on_full_action

    def unsubscribe(self, publisher: Publisher[Type]) -> None:
        if not publisher:
            raise ValueError("Publisher must be supplied")
        with self._publishers_lock:
            logger.debug("%s: Unsubscribing buffer '%s'", self._name, publisher.buffer_name())
            if publisher not in self._publishers:
                raise RuntimeError("Publisher is not subscribed")
            del self._publishers[publisher]

    def on_value(self, value: Type) -> None:
        subscriptions_copy = self._get_copy_of_subscriptions()
        if subscriptions_copy:
            for subscription, on_full_action in subscriptions_copy.items():
                logger.debug("%s: Pushing item '%s' to buffer '%s'", self._name, str(value), subscription.buffer_name())
                try:
                    subscription.publish_value(value, TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
                except queue.Full:
                    self._handle_buffer_full_exception(on_full_action, subscription)
        else:
            logger.debug("%s: Discarding item '%s' - no subscribers", self._name, str(value))

    def on_complete(self, error: Optional[BaseException]) -> None:
        subscriptions_copy = self._get_copy_of_subscriptions()
        if subscriptions_copy:
            for subscription, on_full_action in subscriptions_copy.items():
                logger.debug("%s: Pushing Completion (with error '%s') to buffer '%s'", self._name, safe_str(error), subscription.buffer_name())
                try:
                    try:
                        subscription.publish_complete(error, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
                    except queue.Full:
                        self._handle_buffer_full_exception(on_full_action, subscription)
                except queue.Full:  # on_full_action must be "raise exception"
                    if error:
                        logger.warning(f"Buffer full in on_complete (with an error '{error}'). "
                                       f"The multicaster will continue distributing the original error to any remaining subscriptions.")
                    else:
                        # from now on, we are dealing with this error situation
                        logger.error(f"Buffer full in on_complete. The multicaster will distribute this error to any remaining subscriptions.")
                        raise
        else:
            logger.debug("%s: Discarding Completion (with error '%s') - no subscribers", self._name, safe_str(error))

    def _get_copy_of_subscriptions(self) -> Dict[Publisher[Type], UnexpectedSituationAction]:
        with self._publishers_lock:
            return self._publishers.copy()

    def _handle_buffer_full_exception(self, on_full_action: UnexpectedSituationAction, subscription: Publisher[Type]) -> None:
        handle_unexpected_situation(on_full_action, f"{self._name}: Unable to push to buffer '{subscription.buffer_name()}', it is full", logger,
                                    exception_factory=lambda s: queue.Full(s))  # if on_full_action=RAISE_EXCEPTION, re-raise queue.Full rather than RuntimeError
