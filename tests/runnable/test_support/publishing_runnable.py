import logging
import queue
import time
from enum import Enum, auto, unique
from typing import Optional, TypeVar, cast

from puma.attribute import child_only, child_scope_value
from puma.buffer import Publishable, Publisher
from puma.primitives import AutoResetEvent
from puma.runnable import Runnable
from puma.runnable.message import CommandMessage
from puma.timeouts import TIMEOUT_NO_WAIT
from puma.unexpected_situation_action import UnexpectedSituationAction
from tests.runnable.test_support.testval import TestVal

Type = TypeVar("Type")

logger = logging.getLogger(__name__)


@unique
class PublishingRunnableMode(Enum):
    PushRegularly = auto()  # Push then sleep; error if buffer is full
    PushHard = auto()  # Push blocking in tight loop to keep buffer full

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}.{self.name}>"


class PublishingRunnable(Runnable):
    _count: int = child_only("_count")
    _publisher: Publisher[TestVal] = child_only("_publisher")
    _do_error_at: int = child_only("_do_error_at")
    _mode: PublishingRunnableMode = child_only("_mode")
    _delay: Optional[float] = child_only("_delay")
    _send_complete: bool = child_only("_send_complete")
    _error_if_full: bool = child_only("_error_if_full")
    _large_objects: bool = child_only("_large_objects")

    def __init__(self, count: int, publishable: Publishable[TestVal], *,
                 do_error: bool = False,
                 mode: PublishingRunnableMode = PublishingRunnableMode.PushRegularly,
                 delay: Optional[float] = None,
                 error_if_full: Optional[bool] = None,
                 send_complete: bool = True,
                 large_objects: bool = False) -> None:
        name = "Publisher on " + publishable.buffer_name()
        if mode == PublishingRunnableMode.PushRegularly and (delay is None or delay <= 0.0):
            raise ValueError("A delay, greater than zero, must be supplied if mode is PushRegularly")
        elif mode == PublishingRunnableMode.PushHard and delay is not None:
            raise ValueError("Do not supply a delay value if mode is PushHard")

        if error_if_full is None:
            if mode == PublishingRunnableMode.PushHard:
                self._error_if_full = child_scope_value(False)
            else:
                self._error_if_full = child_scope_value(True)
        else:
            self._error_if_full = child_scope_value(error_if_full)

        super().__init__(name, [publishable])
        self._count = child_scope_value(count)
        self._publisher = self._get_publisher(publishable)
        self._do_error_at = child_scope_value(count // 2 if do_error else -1)
        self._mode = child_scope_value(mode)
        self._delay = child_scope_value(delay)
        self._send_complete = child_scope_value(send_complete)
        self._large_objects = child_scope_value(large_objects)

    def _execute(self) -> None:
        logger.debug("%s: Started", self._name)
        try:
            command_buffer = self._get_command_message_buffer()
            event = AutoResetEvent()
            last_published_at = time.monotonic()
            with command_buffer.subscribe(event) as command_subscription:
                i = 0
                while i < self._count and not self._stop_task:
                    if i == self._do_error_at:
                        logger.debug("%s: Raising test error", self._name)
                        raise RuntimeError("Test Error")

                    published_value = self._push_value(i)

                    if published_value:
                        last_published_at = time.monotonic()
                        logger.debug("%s: Published value %d", self._name, i)
                        i += 1

                    if self._mode == PublishingRunnableMode.PushRegularly:
                        time.sleep(cast(float, self._delay))

                    logger.debug("%s: Polling command queue", self._name)
                    while not self._stop_task:
                        try:
                            command_subscription.call_events(self._on_command)
                        except queue.Empty:
                            logger.debug("%s: No more commands", self._name)
                            break

                    if time.monotonic() - last_published_at > 1.0:
                        logger.debug("Haven't managed to publish anything for a while, giving up")
                        break
        except Exception as ex:
            if self._send_complete:
                logger.debug("%s: Error: '%s'. Publishing Completion with error", str(ex), self._name)
                self._publisher.publish_complete(error=ex, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            else:
                logger.debug("%s: Error: '%s' being raised, but NOT sending Completion", str(ex), self._name)
        else:
            if self._send_complete:
                logger.debug("%s: Publishing Completion without error", self._name)
                self._publisher.publish_complete(error=None, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            else:
                logger.debug("%s: NOT sending Completion", self._name)
        finally:
            logger.debug("%s: Finished", self._name)

    def _on_command(self, value: CommandMessage) -> None:
        logger.debug("%s: Got command %s from command queue", self._name, str(value))
        self._handle_command(value)

    def _push_value(self, val: int) -> bool:
        logger.debug("%s: Publishing value %d", self._name, val)
        if self._mode == PublishingRunnableMode.PushRegularly:
            try:
                self._publisher.publish_value(TestVal(val, time.monotonic(), large=self._large_objects), TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
                return True
            except queue.Full:
                if self._error_if_full:
                    raise
                else:
                    logger.debug("Buffer full, ignoring it")
                    return False
        elif self._mode == PublishingRunnableMode.PushHard:
            try:
                self._publisher.publish_value(TestVal(val, time.monotonic(), large=self._large_objects), 0.1, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
                return True
            except queue.Full:
                if self._error_if_full:
                    raise
                else:
                    logger.debug("Buffer push timed out, retrying")
                    return False
        else:
            raise ValueError("Unrecognised value of mode parameter")
