import logging
from abc import ABC, abstractmethod
from contextlib import ExitStack
from typing import Any, Callable, Optional, Set, Type, TypeVar

from puma.attribute import AccessibleScope, parent_only
from puma.attribute.attribute.scoped_attribute import ScopedAttribute
from puma.attribute.mixin import ScopedAttributesBaseMixin, ScopedAttributesCompatibilityMixin
from puma.buffer import Buffer
from puma.context import ContextManager, Exit_1, Exit_2, Exit_3, ensure_used_within_context_manager, must_be_context_managed
from puma.runnable import Runnable
from puma.runnable.message import CommandMessage, CommandMessageBuffer, StatusBuffer, StatusBufferSubscription, StatusMessage, StatusMessageBuffer
from puma.timeouts import TIMEOUT_INFINITE, Timeouts

DEFAULT_COMMAND_AND_STATUS_BUFFER_SIZE = 10
DEFAULT_FINAL_JOIN_TIMEOUT = 30.0

BufferType = TypeVar("BufferType")

logger = logging.getLogger(__name__)


@must_be_context_managed
class Runner(ScopedAttributesCompatibilityMixin, ContextManager["Runner"], ABC):
    """Base class for ThreadRunner and ProcessRunner. Executes a Runnable in either a thread or a process."""
    _context_management: ExitStack = parent_only("_context_management")

    def __init__(self, runnable: Runnable, name: Optional[str] = None) -> None:
        """Constructor.

        Arguments:
            runnable: The Runnable to execute.
            name: Optional name for the Runner, used for logging. If None, a name is created from the runnable's name. For example, if the Runnable is called “GUI” and
                  this is run in a ThreadRunner then that Runner will be called ‘ThreadRunner of GUI” if no name is supplied.

        Create objects in a context managed way. Use start() or start_blocking() to start the runner.
        The runner will end if stop() is called. Some runners will also end in other circumstances e.g. if they receive on_complete from an input buffer.
        Wait for the runner to end using join().
        When context management ends, stop() and join() are called.
        """
        super().__init__()
        name_to_use = name or self.__class__.__name__ + " of " + runnable.get_name()
        self.set_name(name_to_use)
        if not runnable:
            raise ValueError("A runner must be supplied with a runnable")
        self._runnable = runnable
        self._command_buffer = self._create_command_message_buffer()
        self._wrapped_status_buffer = self._create_status_message_buffer()
        self._status_buffer = StatusBuffer(self._wrapped_status_buffer)
        self._status_buffer_subscription: Optional[StatusBufferSubscription] = None
        self._context_management = ExitStack()
        self._in_context_management = False

    def __enter__(self) -> 'Runner':
        logger.debug("%s: Entering context management (call start or start_blocking to start running)", self.get_name())
        self._context_management.enter_context(self._command_buffer)
        self._context_management.enter_context(self._wrapped_status_buffer)
        self._status_buffer_subscription = self._status_buffer.subscribe()
        self._context_management.enter_context(self._status_buffer_subscription)
        self._runnable.runner_accessor.set_command_publisher(self._command_buffer.publish())
        self._in_context_management = True
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        """End of context management. The runner is stopped and then join() is called.

        If any errors have occurred then they will be re-raised; although it is good practice for the caller not to rely on this. Instead, callers should
        poll check_for_exceptions(), otherwise they will not know that the runner has ended and they should exit its context management.
        """
        self._in_context_management = False
        unrolling_after_exception: bool = exc_type is not None
        logger.debug("%s: Exiting context management, stopping", self.get_name())
        try:
            if self.is_alive():
                self._runnable.stop()
                try:
                    self.join(self._get_final_join_timeout())
                except RunnerStillAliveError as e:
                    if not unrolling_after_exception:
                        raise e
                    else:
                        # Ignore error if already unrolling due to an exception
                        pass
            if not unrolling_after_exception:
                self.check_for_exceptions()
        finally:
            try:
                self._runnable.runner_accessor.close_command_publisher()
                self._context_management.__exit__(exc_type, exc_value, traceback)
                self._status_buffer_subscription = None
            finally:
                logger.debug("%s: Finished", self.get_name())

    @ensure_used_within_context_manager
    def start_blocking(self, timeout: float = TIMEOUT_INFINITE) -> None:
        """Starts the thread/process and then blocks until the Started status has been received from it, indicating that it has really started.

        Raises RuntimeError if the status message is not received within the given timeout.
        """
        logger.debug("%s: Starting", self.get_name())
        Timeouts.validate(timeout)
        self.start()
        self.wait_until_running(timeout)

    @ensure_used_within_context_manager
    def wait_until_running(self, timeout: float = TIMEOUT_INFINITE) -> None:
        """Blocks until the Started status has been received from the thread/process, indicating that it has really started.

        Raises RuntimeError if the status message is not received within the given timeout.
        Raises an exception if the Runner pushed an error onto the queue.
        """
        logger.debug("%s: Waiting until running", self.get_name())
        Timeouts.validate(timeout)
        if not self._status_buffer_subscription:
            raise RuntimeError(f"{self.get_name()}: Not context managed")
        if not self._status_buffer_subscription.block_until_running(timeout):
            raise RuntimeError(f"{self.get_name()}: Failed to start within the given timeout")

    def run(self) -> None:
        """The method executed by the thread/process. Calls the runnable, and sends basic messages back to the owner on the status buffer.

        Does not use the command buffer; the runnable must monitor for the stop command.
        """
        with self._status_buffer.publish() as status_buffer_publisher:
            self._runnable.runner_accessor.set_status_buffer_publisher(status_buffer_publisher)

            logger.debug("%s: Started", self.get_name())
            self._runnable.runner_accessor.record_child_scope_id()

            try:
                self._pre_run_execute()
                self._runnable.runner_accessor.publish_started_status_message()
                self._runnable.runner_accessor.run_execute()
            except Exception as ex:
                logger.error(f"{self.get_name()}: Stopped because of error: {repr(ex)}", exc_info=True)
                if self._in_context_management:
                    status_buffer_publisher.publish_complete(error=ex)  # Transport error back to caller, who will re-raise it when they call check_for_exceptions
            else:
                logger.debug("%s: Stopped OK", self.get_name())
                if self._in_context_management:
                    status_buffer_publisher.publish_complete(error=None)
            finally:
                self._runnable.runner_accessor.set_status_buffer_publisher(None)

    def _pre_run_execute(self) -> None:
        """Callback that runs within the error handling of the runner but before the runnable's _run_execute is called"""
        self._handle_scoped_attributes_in_child_scope(self._runnable, set())

    @ensure_used_within_context_manager
    def stop(self) -> None:
        """Sends the stop command on the command buffer to the runnable."""
        self._runnable.stop()

    @staticmethod
    def _get_command_and_status_buffer_size() -> int:
        """Returns the buffer size for the command buffer and status buffer."""
        return DEFAULT_COMMAND_AND_STATUS_BUFFER_SIZE

    @staticmethod
    def _get_final_join_timeout() -> float:
        """Returns the timeout for the join() called when the runnable exits context management."""
        return DEFAULT_FINAL_JOIN_TIMEOUT

    def _create_command_message_buffer(self) -> CommandMessageBuffer:
        """Factory method creating the command message buffer."""
        return self._buffer_factory(CommandMessage, self._get_command_and_status_buffer_size(), "Command buffer on " + self.get_name(), False)

    def _create_status_message_buffer(self) -> StatusMessageBuffer:
        """Factory method creating the status message buffer."""
        return self._buffer_factory(StatusMessage, self._get_command_and_status_buffer_size(), "Status buffer on " + self.get_name(), False)

    @ensure_used_within_context_manager
    def check_for_exceptions(self) -> None:
        """Pops status messages from the status message buffer, and if any of them are errors then raises the error."""
        if not self._status_buffer_subscription:
            raise RuntimeError(f"{self.get_name()}: Not context managed")
        self._status_buffer_subscription.check_for_exceptions()

    @abstractmethod
    @ensure_used_within_context_manager
    def start(self) -> None:
        """Starts the thread/process. Normally satisfied in a derived class by delegating to the Thread or Process, both of which implement start()."""
        raise NotImplementedError

    @ensure_used_within_context_manager
    def join(self, timeout: Optional[float] = None) -> None:
        """Blocks until the thread/process has ended."""
        Timeouts.validate_optional(timeout)

        self._perform_join(timeout)

        # Raise an error if the Thread or Process is still running
        if self.is_alive():
            raise RunnerStillAliveError(f"Failed to stop the runner: {self.get_name()}")

    @abstractmethod
    def _perform_join(self, timeout: Optional[float] = None) -> None:
        # Actually perform join, usually by delegating to Thread or Process
        raise NotImplementedError()

    @abstractmethod
    def is_alive(self) -> bool:
        """Tests if the thread/process is running. Normally satisfied in a derived class by delegating to the Thread or Process, both of which implement is_alive()."""
        raise NotImplementedError()

    @abstractmethod
    def get_name(self) -> str:
        """Returns the name of the thread/process. Normally satisfied in a derived class by delegating to the Thread or Process, both of which implement get_name().
        The name is set using set_name() in the constructor.
        """
        raise NotImplementedError()

    @abstractmethod
    def set_name(self, name: str) -> None:
        """Sets the name of the thread/process. Normally satisfied in a derived class by also delegating to the Thread or Process, both of which implement set_name().
        Called by the constructor.
        """
        raise NotImplementedError()

    @abstractmethod
    def _buffer_factory(self, element_type: Type[BufferType], size: int, name: str, warn_on_discard: Optional[bool] = True) -> Buffer[BufferType]:
        """Returns the type of Buffer to use to communicate within this Runner; ie a MultiThreadBuffer or a MultiProcessBuffer"""
        raise NotImplementedError()

    def _iterate_over_all_scoped_attributes(self, obj: Any, callback: Callable[[str, ScopedAttribute], None]) -> None:
        for i in type(obj).mro():
            for name, attribute in i.__dict__.items():
                if isinstance(attribute, ScopedAttribute):
                    callback(name, attribute)

    def _handle_scoped_attributes_in_child_scope(self, obj: Any, object_recursion_tracker: Set[Any]) -> None:

        def attribute_callback(name: str, attribute: ScopedAttribute) -> None:
            if attribute.accessible_scope != AccessibleScope.parent:
                self._handle_individual_scoped_attribute_in_child_scope(obj, name, attribute)
                attribute_value = getattr(obj, name)
                # Recursively handle ScopedAttributes whilst avoiding infinite loops
                if isinstance(attribute_value, ScopedAttributesBaseMixin) and attribute_value not in object_recursion_tracker:
                    object_recursion_tracker.add(attribute_value)
                    self._handle_scoped_attributes_in_child_scope(attribute_value, object_recursion_tracker)

        self._iterate_over_all_scoped_attributes(obj, attribute_callback)

    def _handle_individual_scoped_attribute_in_child_scope(self, obj: Any, name: str, attribute: ScopedAttribute) -> None:
        # Do nothing by default
        pass


class RunnerStillAliveError(RuntimeError):
    # An error that indicates that a runner was still alive after "join" has returned
    pass
