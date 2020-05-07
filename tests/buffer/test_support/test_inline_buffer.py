from typing import TypeVar

from puma.context import Exit_1, Exit_2, Exit_3
from puma.helpers.testing.mixin import NotATestCase
from tests.buffer.test_support.single_threaded_primitive_types import InlineBuffer

Type = TypeVar("Type")


class TestInlineBuffer(InlineBuffer[Type], NotATestCase):
    """A test implementation of Buffer that is non-blocking (has no locking or timeouts), is instantaneous, and is not necessarily thread or process safe,
    and records calls to __enter__ and __exit__, for testing.
    """

    def __init__(self, size: int, name: str):
        super().__init__(size, name)
        self.entered = False
        self.exited = False

    def __enter__(self) -> 'TestInlineBuffer[Type]':
        self.entered = True
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        self.exited = True
