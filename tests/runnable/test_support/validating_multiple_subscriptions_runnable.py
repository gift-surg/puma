import logging
from typing import Any, List, TypeVar

from puma.buffer import Observable
from puma.runnable import MultiBufferServicingRunnable
from tests.runnable.test_support.validating_subscriber import ValidatingSubscriber

Type = TypeVar("Type")

logger = logging.getLogger(__name__)


class ValidatingMultipleSubscriptionsRunnable(MultiBufferServicingRunnable):
    def __init__(self, observables: List[Observable[Any]], num_expected: int, name: str) -> None:
        super().__init__(name, [])
        for observable in observables:
            self._add_subscription(observable, ValidatingSubscriber(observable.buffer_name(), num_expected))
