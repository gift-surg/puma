from abc import ABC
from typing import ContextManager as TypingContextManager, Generic, TypeVar

from puma.context.types import Exit_1, Exit_2, Exit_3

T = TypeVar("T")


class ContextManager(Generic[T], TypingContextManager[T], ABC):

    def __enter__(self) -> T:
        pass

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        """
        Whilst the true interface of ContextManager.__exit__ requires a return of Optional[bool], this is only useful in a very specific subset of cases
        and has been found to cause more issues than it solves
        """
        pass
