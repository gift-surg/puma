import logging
from typing import Optional, TypeVar

from puma.attribute import unmanaged
from puma.buffer import Observable
from puma.runnable import SingleBufferServicingRunnable
from tests.runnable.test_support.testval import TestVal
from tests.runnable.test_support.validating_subscriber import ValidatingSubscriber, ValidatingSubscriberMode

Type = TypeVar("Type")

logger = logging.getLogger(__name__)


class ValidatingSingleSubscriptionRunnable(SingleBufferServicingRunnable[TestVal]):
    _subscriber: ValidatingSubscriber = unmanaged("_subscriber")

    def __init__(self,
                 observable: Observable[TestVal],
                 num_expected: int,
                 mode: ValidatingSubscriberMode = ValidatingSubscriberMode.PopFast,
                 delay: Optional[float] = None,
                 *,
                 should_receive_all: bool = True,
                 raise_received_completion_errors: bool = True) -> None:
        self._subscriber = ValidatingSubscriber(observable.buffer_name(), num_expected, mode=mode, delay=delay,
                                                should_receive_all=should_receive_all, raise_received_completion_errors=raise_received_completion_errors)
        name = "Validating " + observable.buffer_name()
        super().__init__(observable, self._subscriber, [], name)

    def _execution_ending_hook(self, error: Optional[Exception]) -> bool:
        self._subscriber.runnable_is_ending()
        return False  # Error has not been handled
