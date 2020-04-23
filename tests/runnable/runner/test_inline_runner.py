from typing import Any, Optional, Set

from puma.context import ensure_used_within_context_manager
from puma.runnable.message import StatusMessage
from tests.buffer.test_support.single_threaded_primitive_types import InlineRunner
from tests.buffer.test_support.test_inline_buffer import TestInlineBuffer
from tests.mixin import NotATestCase
from tests.runnable.runner.test_inline_runnable import TestInlineRunnable

TST_BUFFER_SIZES = 13


class TestInlineRunner(InlineRunner, NotATestCase):
    """A test implementation of Runner that does not launch a separate thread or process but instead executes the runnable in the caller's thread,
    and behaves as if it has instantaneous command and status buffers.
    """

    def __init__(self, runnable: TestInlineRunnable, name: Optional[str] = None) -> None:
        self._test_runnable = runnable
        self.joined = False
        # call super's init last because our member variables are there to monitor super's behaviour, including its constructor's behaviour
        super().__init__(runnable, name)  # should call our set_name() and hence set self._name

    def __enter__(self) -> 'TestInlineRunner':
        super().__enter__()
        return self

    def _handle_scoped_attributes_in_child_scope(self, obj: Any, object_recursion_tracker: Set[Any]) -> None:
        # Do nothing, preventing issues with accessing child_only attributes when in the same thread as the parent
        pass

    @ensure_used_within_context_manager
    def join(self, timeout: Optional[float] = None) -> None:
        super().join(timeout)
        self.joined = True

    def is_alive(self) -> bool:
        return self._test_runnable.executed and not self._test_runnable.stopped

    @staticmethod
    def _get_command_and_status_buffer_size() -> int:
        return TST_BUFFER_SIZES

    def _create_status_message_buffer(self) -> TestInlineBuffer[StatusMessage]:
        self.wrapped_status_buffer: TestInlineBuffer = TestInlineBuffer[StatusMessage](self._get_command_and_status_buffer_size(), "Status buffer on " + self.get_name())
        return self.wrapped_status_buffer

    def get_status_buffer(self) -> TestInlineBuffer[StatusMessage]:
        if not self.wrapped_status_buffer:
            raise RuntimeError("Error: buffer should have been created")
        return self.wrapped_status_buffer
