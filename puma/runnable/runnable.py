import logging
from abc import ABC, abstractmethod
from contextlib import ExitStack
from typing import Any, Callable, Collection, Dict, List, Optional, Type, TypeVar, Union

from puma.attribute import ProcessAction, ThreadAction, child_only, child_scope_value, copied, factory, manually_managed, per_scope_value, python_default, unmanaged
from puma.attribute.mixin import ScopedAttributesMixin
from puma.buffer import Publishable, Publisher
from puma.helpers.assert_set import assert_set
from puma.runnable._in_runnable_indirect_publisher import _InRunnableIndirectPublisher
from puma.runnable.message import CommandMessage, CommandMessageBuffer, RemoteObjectGetAttributeCommandMessage, RemoteObjectMethodCommandMessage, RunInChildScopeCommandMessage, \
    RunInChildScopeStatusMessage, StartedStatusMessage, StatusBuffer, StatusBufferPublisher, StatusBufferSubscription, StatusMessage, StopCommandMessage
from puma.runnable.publishable_to_publisher_mapping import PublishableToPublisherMapping
from puma.runnable.remote_execution import AutoRemoteObjectReferencePlaceholder, RemoteObjectResultReference
from puma.scope_id import get_current_scope_id
from puma.timeouts import TIMEOUT_INFINITE
from puma.unexpected_situation_action import UnexpectedSituationAction

logger = logging.getLogger(__name__)

SEND_COMMAND_TIMEOUT = 10
OUTPUT_BUFFERS_MODIFIED_WHILE_RUNNING_ERROR = "Output buffers may not be added or removed while the runnable is executing"

AStatusMessageType = TypeVar("AStatusMessageType", bound=StatusMessage)
AType = TypeVar("AType")
PType = TypeVar("PType")

PRIMITIVE_TYPES = str, float, int, bool, type(None), list, dict
ResultType = Union[str, float, int, bool, None, list, dict, RemoteObjectResultReference]

AttributeHandler = Callable[[str], Any]
MethodHandler = Callable[..., Any]
AttributeOrMethodHandlerType = TypeVar("AttributeOrMethodHandlerType", AttributeHandler, MethodHandler)


class Runnable(ScopedAttributesMixin, ABC):
    """Base class for tasks that are executed by a Runner.

    A Runnable can be executed in either a separate thread or process, depending on which Runner is used (ThreadRunner or ProcessRunner).
    The Runner is responsible for sending basic status messages (started, finished) to the owner, but the Runnable must monitor and respond to commands (including Stop)
    on its command buffer. Derived classes support the common cases where a Runner needs to service one or more input buffers as well as monitoring its command buffer.
    """
    _name: str = copied("_name")
    _stop_task: bool = copied("_stop_task")
    __command_buffer: Optional[CommandMessageBuffer] = unmanaged("__command_buffer")
    __status_buffer: Optional[StatusBuffer] = unmanaged("__status_buffer")
    _child_scope_id: str = copied("_child_scope_id")
    __indirect_publishers: PublishableToPublisherMapping = unmanaged("__indirect_publishers")
    __executing: bool = unmanaged("__executing")
    _command_publisher: Optional[Publisher[CommandMessage]] = manually_managed("_command_publisher", ThreadAction.SHARED, ProcessAction.SET_TO_NONE)
    _status_publisher: Optional[StatusBufferPublisher] = copied("_status_publisher")
    _status_subscription: Optional[StatusBufferSubscription] = python_default("_status_subscription")
    _remote_result_cache: Dict[str, Any] = child_only("_remote_result_cache")

    runner_accessor: "RunnerAccessor" = copied("runner_accessor")
    run_in_child_scope_accessor: "RunInChildScopeAccessor" = copied("run_in_child_scope_accessor")
    multicaster_accessor: "MulticasterAccessor" = copied("multicaster_accessor")

    def __init__(self, name: str, output_buffers: Collection[Publishable[Any]]) -> None:
        """Constructor.

        Arguments:
            name: A name for the Runnable, used for logging.
            output_buffers:  The outputs that this runnable has. Use get_publisher to publish values.
        """
        super().__init__()
        if not name:
            raise ValueError("A name must be provided for a runnable")
        if output_buffers is None:
            raise ValueError("output_buffers array must be provided; pass an empty array if the runnable has no output buffers")
        self._name = name
        self._stop_task = False
        self.__command_buffer = None
        self.__status_buffer = None
        self._child_scope_id = "unknown"
        self.__indirect_publishers = PublishableToPublisherMapping()
        self.__executing = False
        self._status_publisher = per_scope_value(None)
        self._status_subscription = None
        self._remote_result_cache = child_scope_value({})
        self._command_publisher = per_scope_value(None)

        self.runner_accessor = factory(self._create_runner_accessor)
        self.run_in_child_scope_accessor = factory(self._create_run_in_child_scope_accessor)
        self.multicaster_accessor = factory(self._create_multicaster_accessor)

        for output_buffer in output_buffers:
            self._add_output_buffer(output_buffer)

    def _create_runner_accessor(self) -> "RunnerAccessor":
        return RunnerAccessor(self)

    def _create_run_in_child_scope_accessor(self) -> "RunInChildScopeAccessor":
        return RunInChildScopeAccessor(self)

    def _create_multicaster_accessor(self) -> "MulticasterAccessor":
        return MulticasterAccessor(self)

    #########################
    #  MulticasterAccessor  #
    #########################
    def _multicaster_accessor__remove_output_buffer(self, output_buffer: Publishable[Any]) -> Publisher[Any]:
        if self.__executing:
            raise RuntimeError(OUTPUT_BUFFERS_MODIFIED_WHILE_RUNNING_ERROR)
        ret = self.__indirect_publishers.pop(hash(output_buffer))
        if ret is None:
            raise RuntimeError("Trying to remove an unknown output buffer")
        return ret

    #########################
    #    RunnerAccessor     #
    #########################
    def _runner_accessor__assert_comms_not_already_set(self) -> None:
        if self.__command_buffer or self._status_publisher:
            raise RuntimeError(f"Communications channels have already been set - are you trying to reuse / restart this Runnable ('{self.get_name()}')?")

    def _runner_accessor__set_command_buffer(self, buffer: CommandMessageBuffer) -> None:
        """Called by the Runner (in the parent process), sets the buffer that the child process should monitor for commands and the parent uses to issue commands."""
        self.__command_buffer = buffer

    def _runner_accessor__set_status_buffer_publisher(self, status_buffer_publisher: Optional[StatusBufferPublisher]) -> None:
        """Called by the runner (in the child process), sets the endpoint that the child process should use to report status, and clears it before the child exits."""
        self._status_publisher = status_buffer_publisher

    def _runner_accessor__set_status_buffer_subscription(self, status_buffer_subscription: Optional[StatusBufferSubscription]) -> None:
        """Called by the runner (in the parent process), sets the endpoint that the parent process should use to monitor status."""
        # The parameters is not really optional, it may not be None; but specifying it as optional makes this method easier to call
        if not status_buffer_subscription:
            raise ValueError("set_status_buffer_subscription parameter may not be None")
        self._status_subscription = status_buffer_subscription

    def _runner_accessor__record_child_scope_id(self) -> None:
        self._child_scope_id = get_current_scope_id()

    def _runner_accessor__set_command_publisher(self, command_publisher: Publisher[CommandMessage]) -> None:
        self._command_publisher = command_publisher
        self._command_publisher.__enter__()

    def _runner_accessor__close_command_publisher(self) -> None:
        if self._command_publisher:
            self._command_publisher.__exit__(None, None, None)
            self._command_publisher = None

    def _add_output_buffer(self, output_buffer: Publishable[Any]) -> Publisher[Any]:
        if self.__executing:
            raise RuntimeError(OUTPUT_BUFFERS_MODIFIED_WHILE_RUNNING_ERROR)
        if hash(output_buffer) in self.__indirect_publishers:
            raise RuntimeError("Output buffer is already added")
        publisher: _InRunnableIndirectPublisher = self._create_in_runnable_indirect_publisher(output_buffer, self.__indirect_publishers.cross_scope_accessor)
        self.__indirect_publishers[hash(output_buffer)] = publisher
        return publisher

    def _runner_accessor__publish_started_status_message(self) -> None:
        self._send_status_message(StartedStatusMessage())

    def _runner_accessor__run_execute(self) -> None:
        # Called by the Runner. Publishes the publishables INSIDE the thread/process, to prevent deadlocks -
        # see BufferBase._launch_discard_after_delay_thread_if_no_publishers_and_no_subscriber_and_buffer_not_empty.
        # By entering and exiting the publishables inside the child process, the buffer will be emptied if it has no subscribers.
        # Concrete runnables implement _execute(), and use get_publisher() to access the publishers they need.
        self._check_ready_to_execute()
        try:
            self.__executing = True
            with ExitStack() as buffers_exit_stack:
                # Publish to the output buffers, and store these real publishers in the indirect publishers obtainable with get_publisher() while executing
                for publishable_id, indirect_publisher in self.__indirect_publishers.items():
                    publisher = indirect_publisher._publishable.publish()
                    indirect_publisher.set_publisher(publisher)
                    buffers_exit_stack.enter_context(indirect_publisher)
                self._execute()
        finally:
            self.__executing = False

    ###############################
    #  RunInChildScopeAccessor  #
    ###############################
    def _run_in_child_scope_accessor__is_in_child_context(self) -> bool:
        return self._child_scope_id == get_current_scope_id()

    def _run_in_child_scope_accessor__has_command_buffer(self) -> bool:
        return self.__command_buffer is not None

    def _create_in_runnable_indirect_publisher(self, output_buffer: Publishable[Any], publisher_accessor: Callable[[int], Publisher]) -> _InRunnableIndirectPublisher[Any]:
        return _InRunnableIndirectPublisher(output_buffer, publisher_accessor)

    def get_name(self) -> str:
        """Returns the name given to the constructor."""
        return self._name

    def _check_ready_to_execute(self) -> None:
        # Raises an exception if the runnable's environment has not been correctly set by the runner.
        if not self.__command_buffer:
            raise RuntimeError("set_command_buffer has not been called")
        if not self._status_publisher:
            raise RuntimeError("set_status_buffer_publisher has not been called")

    @abstractmethod
    def _execute(self) -> None:
        """The blocking method that is overridden in a derived Runnable to implement the functionality of that Runnable. It is executed in a separate thread or process.

        This method must monitor the command queue (use get_command_message_buffer() to access it) and respond to commands, including the stop command.
        If the Runnable monitors one or more input buffers then it should also stop when it has received on_complete from all of those buffers.

        If the _execute method hits a fatal error, it should raise an exception, which will be handled by the Runner.
        """
        raise NotImplementedError()

    def _get_publisher(self, buffer: Publishable[AType]) -> Publisher[AType]:
        return child_scope_value(self._get_publisher_unwrapped(buffer))

    def _get_publisher_unwrapped(self, buffer: Publishable[AType]) -> Publisher[AType]:
        """Obtains a Publisher which the derived runnable can used to publish values on the given output buffer.

        This should be used inside a runnable, rather than using the publish() method on the publishable.
        Method son the returned object (except for buffer_name()) will raise an exception if they are called when the runnable is not executing.
        """
        publisher = self.__indirect_publishers.get(hash(buffer))
        if publisher is None:
            raise RuntimeError("The buffer passed to get_publisher() was not included in the output_buffers collection passed to the constructor or added with add_output_buffer")
        return publisher  # This is a proxy to the real publisher, which can only be used inside execute()

    def get_latest_status_message(self, status_message_type: Type[AStatusMessageType]) -> Optional[AStatusMessageType]:
        """ Returns the latest instance of the StatusMessage that matches the given type, or None if no messages of that type have yet been retrieved."""
        if not self._status_subscription:
            raise RuntimeError("set_status_buffer_subscription has not been called")
        return self._status_subscription.get_latest_status_message(status_message_type)

    def wait_for_status_message(self, status_message_type: Type[AStatusMessageType], timeout: float = TIMEOUT_INFINITE) -> AStatusMessageType:
        """Blocks until a status message of the type given is received, or until the runnable ends or a timeout expires.

        Returns the latest instance of the StatusMessage that matches the given type, or raises a TimeoutError if no new message of that type has been received within the timeout.
        Once a message of a given type has been returned to the caller, it is deleted and will not be returned again.
        """
        if not self._status_subscription:
            raise RuntimeError("set_status_buffer_subscription has not been called")
        return self._status_subscription.wait_for_status_message(status_message_type, timeout)

    def stop(self) -> None:
        """Signal that the Runnable should be stopped by sending it a stop command on its command buffer."""
        self._send_command(StopCommandMessage())

    def _send_command(self, command: CommandMessage) -> None:
        # Called by the user of the runnable to send a command to it

        def publish_command(publisher: Publisher[CommandMessage]) -> None:
            publisher.publish_value(command, SEND_COMMAND_TIMEOUT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)

        # Allow _send_command to be used from either the parent or child scope
        if self._run_in_child_scope_accessor__is_in_child_context():
            with self._get_command_message_buffer().publish() as temp_publisher:
                publish_command(temp_publisher)
        else:
            publish_command(assert_set(self._command_publisher))

    def _send_status_message(self, status_message: StatusMessage) -> None:
        # Called by the runnable to send status back to its user
        if not self._status_publisher:
            raise RuntimeError("Can't send a status message when not running")
        self._status_publisher.publish_status(status_message)

    def _handle_command(self, command: CommandMessage) -> None:
        """Default implementation that _execute() may call to handle commands received on its command buffer."""
        if isinstance(command, StopCommandMessage):
            self._handle_stop_command()
        elif isinstance(command, (RemoteObjectMethodCommandMessage, RemoteObjectGetAttributeCommandMessage)):
            try:
                if isinstance(command, RemoteObjectGetAttributeCommandMessage):
                    self._handle_remote_object_get_attribute_command_message(command, self._status_publisher)
                else:
                    self._handle_run_in_child_scope_command(command, self._status_publisher)
            except Exception as e:
                assert_set(self._status_publisher).publish_complete(e)
                # TODO: Should this error also be reraised? Should the runnable be allowed to continue?
        else:
            raise RuntimeError(f"Could not handle unknown command: {command}")

    def _handle_remote_object_get_attribute_command_message(self, command: RemoteObjectGetAttributeCommandMessage, status_publisher: Optional[StatusBufferPublisher]) -> None:

        self._handle_remote_object_attribute_or_method(command, status_publisher, lambda attr: attr)

    def _handle_run_in_child_scope_command(self, command: RemoteObjectMethodCommandMessage, status_publisher: Optional[StatusBufferPublisher]) -> None:

        # Filter out AutoRemoteObjectReferencePlaceholder
        args: List[Any] = command.args
        if len(args) > 0 and isinstance(args[0], AutoRemoteObjectReferencePlaceholder):
            args = args[1:]

        self._handle_remote_object_attribute_or_method(command, status_publisher, lambda method: method(*args, **command.kwargs))

    def _handle_remote_object_attribute_or_method(self, command: RunInChildScopeCommandMessage, status_publisher: Optional[StatusBufferPublisher],
                                                  attribute_or_method_handler: AttributeOrMethodHandlerType) -> None:

        # Check if we're acting against the original proxied object, or the result of a previous call
        remote_object_id = command.remote_object_id
        if not remote_object_id:
            attribute_or_method = self._retrieve_remote_attribute_or_method(command.name)
        else:
            attribute_or_method = getattr(self._remote_result_cache[remote_object_id], command.name)

        result = attribute_or_method_handler(attribute_or_method)

        # If result is "primitive", publish it directly. Otherwise, publish a RemoteObjectResultReference
        if isinstance(result, PRIMITIVE_TYPES):
            result_wrapper: RunInChildScopeStatusMessage[ResultType] = RunInChildScopeStatusMessage(command.call_id, result)
        else:
            # Store a reference to the result
            self._remote_result_cache[command.call_id] = result
            # Wrap up the reference
            result_reference = RemoteObjectResultReference(command.call_id, result)
            result_wrapper = RunInChildScopeStatusMessage(command.call_id, result_reference)

        assert_set(status_publisher).publish_status(result_wrapper)

    def _retrieve_remote_attribute_or_method(self, name: str) -> Any:
        return getattr(self, name)

    def _handle_stop_command(self) -> None:
        """Default behaviour when the stop command is received from the command buffer."""
        self._stop_task = True

    def _get_command_message_buffer(self) -> CommandMessageBuffer:
        """Asserts that set_command_buffer() has been called, and then returns the buffer that it was given."""
        if not self.__command_buffer:
            raise RuntimeError("set_command_buffer has not been called")
        return self.__command_buffer


# Accessors
class BaseAccessor(ABC):

    def __init__(self, runnable: Runnable):
        self._runnable = runnable


# noinspection PyProtectedMember
class MulticasterAccessor(BaseAccessor):

    def add_output_buffer(self, output_buffer: Publishable[Any]) -> Publisher[Any]:
        return self._runnable._add_output_buffer(output_buffer)

    def remove_output_buffer(self, output_buffer: Publishable[Any]) -> Publisher[Any]:
        return self._runnable._multicaster_accessor__remove_output_buffer(output_buffer)


# noinspection PyProtectedMember
class RunnerAccessor(BaseAccessor):

    def assert_comms_not_already_set(self) -> None:
        self._runnable._runner_accessor__assert_comms_not_already_set()

    def record_child_scope_id(self) -> None:
        self._runnable._runner_accessor__record_child_scope_id()

    def run_execute(self) -> None:
        self._runnable._runner_accessor__run_execute()

    def publish_started_status_message(self) -> None:
        self._runnable._runner_accessor__publish_started_status_message()

    def set_command_buffer(self, buffer: CommandMessageBuffer) -> None:
        self._runnable._runner_accessor__set_command_buffer(buffer)

    def set_status_buffer_publisher(self, status_buffer_publisher: Optional[StatusBufferPublisher]) -> None:
        self._runnable._runner_accessor__set_status_buffer_publisher(status_buffer_publisher)

    def set_status_buffer_subscription(self, status_buffer_subscription: Optional[StatusBufferSubscription]) -> None:
        self._runnable._runner_accessor__set_status_buffer_subscription(status_buffer_subscription)

    def set_command_publisher(self, command_publisher: Publisher[CommandMessage]) -> None:
        self._runnable._runner_accessor__set_command_publisher(command_publisher)

    def close_command_publisher(self) -> None:
        self._runnable._runner_accessor__close_command_publisher()


# noinspection PyProtectedMember
class RunInChildScopeAccessor(BaseAccessor):

    def has_command_buffer(self) -> bool:
        return self._runnable._run_in_child_scope_accessor__has_command_buffer()

    def is_in_child_scope(self) -> bool:
        return self._runnable._run_in_child_scope_accessor__is_in_child_context()

    def get_command_message_buffer(self) -> CommandMessageBuffer:
        return self._runnable._get_command_message_buffer()

    def send_command(self, command: CommandMessage) -> None:
        self._runnable._send_command(command)
