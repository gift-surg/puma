from typing import Optional

from puma.buffer import Publisher, Subscriber
from puma.timeouts import TIMEOUT_NO_WAIT
from puma.unexpected_situation_action import UnexpectedSituationAction
from tests.runnable.test_support.testval import TestVal


class ForwardingSubscriber(Subscriber[TestVal]):
    def __init__(self, in_buffer_name: str, out_publisher: Publisher[TestVal], cause_error_at_count: int = -1) -> None:
        self._name = f"Forwarding values from '{in_buffer_name}' to '{out_publisher.buffer_name()}'"
        self._out_publisher = out_publisher
        self._cause_error_at_count = cause_error_at_count

    def on_value(self, value: TestVal) -> None:
        if value.counter == self._cause_error_at_count:
            raise RuntimeError("Test Error!")
        self._out_publisher.publish_value(value, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)

    def on_complete(self, error: Optional[BaseException]) -> None:
        self._out_publisher.publish_complete(error, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
