from threading import Thread
from typing import Any, Optional, Type, TypeVar

from puma.attribute import ThreadAction
from puma.attribute.attribute.scoped_attribute import ScopedAttribute
from puma.attribute.attribute.sharing_attribute_between_scopes_not_allowed_error import SharingAttributeBetweenThreadsNotAllowedError
from puma.buffer import Buffer, MultiThreadBuffer
from puma.context import ensure_used_within_context_manager
from puma.runnable.runner import Runner

BufferType = TypeVar("BufferType")


class ThreadRunner(Runner, Thread):
    """Executes a Runnable within a separate thread."""

    def get_name(self) -> str:
        """Overload, delegating to the thread"""
        return self.name

    def set_name(self, name: str) -> None:
        """Overload, delegating to the thread"""
        if not name:
            raise ValueError("A name must be supplied")
        self.name = name

    @ensure_used_within_context_manager
    def start(self) -> None:
        """Overload, delegating to the thread"""
        self._runnable.runner_accessor.assert_comms_not_already_set()
        self._runnable.runner_accessor.set_command_buffer(self._command_buffer)
        self._runnable.runner_accessor.set_status_buffer_subscription(self._status_buffer_subscription)
        Thread.start(self)

    def _handle_individual_scoped_attribute_in_child_scope(self, obj: Any, name: str, attribute: ScopedAttribute) -> None:
        if attribute.thread_action == ThreadAction.NOT_ALLOWED:
            raise SharingAttributeBetweenThreadsNotAllowedError(name)

    def _perform_join(self, timeout: Optional[float] = None) -> None:
        """Overload, delegating to the thread"""
        Thread.join(self, timeout)

    def is_alive(self) -> bool:
        """Overload, delegating to the thread"""
        return Thread.is_alive(self)

    def _buffer_factory(self, element_type: Type[BufferType], size: int, name: str, warn_on_discard: Optional[bool] = True) -> Buffer[BufferType]:
        return MultiThreadBuffer(size, name, warn_on_discard)
