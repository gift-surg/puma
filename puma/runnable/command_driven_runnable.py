import logging
from abc import ABC
from typing import TypeVar

from puma.buffer import Observable, Subscriber
from puma.runnable import MultiBufferServicingRunnable

T = TypeVar("T")

logger = logging.getLogger(__name__)


class CommandDrivenRunnable(MultiBufferServicingRunnable, ABC):
    """Base class for Runnables that service Commands arriving on their internal Command Buffer, but have no input buffers.

    The _execute() method ends when the runnable receives the stop command.
    """

    def _add_subscription(self, observable: Observable[T], subscriber: Subscriber[T]) -> None:
        raise RuntimeError("CommandDrivenRunnable not expecting to service input buffers, use SingleBufferServicingRunnable or MultiBufferServicingRunnable instead")

    def _all_observables_completed(self) -> bool:
        return False

    def _check_observables_set(self) -> None:
        pass
