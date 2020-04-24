import logging
import time
from enum import Enum, auto, unique
from typing import Optional, TypeVar, cast

from puma.buffer import Subscriber
from puma.helpers.string import safe_str
from tests.runnable.test_support.testval import TestVal

Type = TypeVar("Type")

logger = logging.getLogger(__name__)

MAX_THREADS_LATENCY = 0.05


@unique
class ValidatingSubscriberMode(Enum):
    PopFast = auto()  # Push blocking in tight loop to keep buffer full
    PopRegularly = auto()  # Push then sleep; error if buffer is full

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}.{self.name}>"


class ValidatingSubscriber(Subscriber[TestVal]):
    def __init__(self, buffer_name: str, num_expected: int, *,
                 mode: ValidatingSubscriberMode = ValidatingSubscriberMode.PopFast,
                 delay: Optional[float] = None,
                 should_receive_all: bool = True,
                 raise_received_completion_errors: bool = True) -> None:
        if mode == ValidatingSubscriberMode.PopRegularly and (delay is None or delay <= 0.0):
            raise ValueError("A delay, greater than zero, must be supplied if mode is PopRegularly")
        elif mode == ValidatingSubscriberMode.PopFast and delay is not None:
            raise ValueError("Do not supply a delay value if mode is PopFast")
        self._name = "Validating " + buffer_name
        self._num_expected = num_expected
        self._expecting: int = 0
        self._mode = mode
        self._delay = delay
        self._should_receive_all = should_receive_all
        self._raise_received_completion_errors = raise_received_completion_errors
        self._received_on_complete = False

    def on_value(self, value: TestVal) -> None:
        logger.debug("%s: Got value %d", self._name, value.counter)

        if value.counter != self._expecting:
            message = f"{self._name}: Incorrect value received. Got {value.counter}, expected {self._expecting}"
            logger.error(message)
            raise RuntimeError(message)
        self._expecting += 1

        if self._mode == ValidatingSubscriberMode.PopRegularly:
            time.sleep(cast(float, self._delay))

    def on_complete(self, error: Optional[BaseException]) -> None:
        logger.debug("%s: Got Completion", self._name)
        self._received_on_complete = True
        if error:
            logger.debug(f"{self._name}: Received an error: {safe_str(error)}")
            if self._raise_received_completion_errors:
                raise error
        if self._expecting != self._num_expected:
            self._check_final_count("on_complete")

    def runnable_is_ending(self) -> None:
        self._check_final_count("Runnable ending")
        if not self._received_on_complete:
            message = f"{self._name}: Did not receive on_complete"
            logger.error(message)
            raise RuntimeError(message)

    def _check_final_count(self, situation: str) -> None:
        if self._should_receive_all:
            if self._expecting != self._num_expected:
                message = f"{self._name}: {situation}. Got {self._expecting} items, expected {self._num_expected}."
                logger.error(message)
                raise RuntimeError(message)
        else:
            if self._expecting == self._num_expected:
                message = f"{self._name}: {situation}. Expected it to end early, but got all {self._num_expected} items."
                logger.error(message)
                raise RuntimeError(message)
