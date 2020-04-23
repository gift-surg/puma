import logging
import queue
import threading
import time
from typing import Dict, Optional, Type, TypeVar, Union, cast

from puma.attribute import ProcessAction, ThreadAction, copied, factory, manually_managed, per_scope_value, python_default
from puma.attribute.mixin import ScopedAttributesMixin
from puma.buffer import Publisher, Subscription, TraceableException
from puma.context import ContextManager, Exit_1, Exit_2, Exit_3
from puma.helpers.string import safe_str
from puma.primitives import AutoResetEvent
from puma.runnable.message import RunInChildScopeStatusMessage, StartedStatusMessage, StatusMessage, StatusMessageBuffer
from puma.timeouts import TIMEOUT_INFINITE, TIMEOUT_NO_WAIT, Timeouts
from puma.unexpected_situation_action import UnexpectedSituationAction

logger = logging.getLogger(__name__)

StatusMessageType = TypeVar("StatusMessageType", bound=StatusMessage)


class StatusBuffer(ScopedAttributesMixin):
    """Contains a buffer that a runnable class (thread or process) uses to communicate back to its creator, and methods to use that buffer.

    publish() should be called from the run() method of the runnable class being monitored. subscribe() should be called by the user of the runnable class being monitored.

    The purpose of this wrapper is to provide higher level abstractions than the buffer that it wraps. The wrapped buffer transports status messages from a runnable
    to its creator; this wrapper provides methods such as "block until running" which rely on the behaviour of a Runner (e.g. that it always sends a "running"
    status message just before it executes its Runnable).
    """
    _name: str = copied("_name")
    _wrapped_buffer: StatusMessageBuffer = copied("_wrapped_buffer")

    def __init__(self, wrapped_buffer: StatusMessageBuffer) -> None:
        """Constructor. Given a MultiThreadBuffer[StatusMessage] or a MultiProcessBuffer[StatusMessage], which it wraps."""
        super().__init__()
        if not wrapped_buffer:
            raise ValueError("A wrapped buffer must be provided")
        self._wrapped_buffer: StatusMessageBuffer = wrapped_buffer
        self._name = wrapped_buffer.buffer_name()

    def publish(self) -> 'StatusBufferPublisher':
        return StatusBufferPublisher(self._wrapped_buffer, self)

    def subscribe(self) -> 'StatusBufferSubscription':
        return StatusBufferSubscription(self._wrapped_buffer, self)


class StatusBufferPublisher:
    def __init__(self, wrapped_buffer: StatusMessageBuffer, parent: 'StatusBuffer') -> None:
        self._wrapped_buffer: StatusMessageBuffer = wrapped_buffer
        self._parent = parent
        self._name = wrapped_buffer.buffer_name()
        self._wrapped_publisher: Optional[Publisher[StatusMessage]] = None

    def __enter__(self) -> 'StatusBufferPublisher':
        logger.debug("%s: StatusBufferPublisher Entering context management", self._name)
        if self._wrapped_publisher:
            raise RuntimeError("StatusBufferPublisher already context managed")
        self._wrapped_publisher = self._wrapped_buffer.publish().__enter__()
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        logger.debug("%s: StatusBufferPublisher Exiting context management", self._name)
        if not self._wrapped_publisher:
            raise RuntimeError("StatusBufferPublisher not context managed")
        self._wrapped_publisher.__exit__(exc_type, exc_value, traceback)
        self._wrapped_publisher = None

    def publish_status(self, status: StatusMessage) -> None:
        """Called by the Runner to notify its owner of its status."""
        if not status:
            raise ValueError("Status message must be provided")
        if not isinstance(status, StatusMessage):
            raise ValueError("Status message is not of legal type")
        if not self._wrapped_publisher:
            raise RuntimeError("StatusBufferPublisher not context managed")
        logger.debug("%s: Sending status: %s", self._name, str(status))
        self._wrapped_publisher.publish_value(status, TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.LOG_WARNING)  # Don't raise if full, errors will build up

    def publish_complete(self, error: Optional[Exception]) -> None:
        """Called by the Runner to notify its owner that it has finished, including an optional fatal error.

        The error parameter is optional (None if there is no error) but is not defaulted to None: the caller must explicitly state that it is None if there is no error, this is to
        encourage the caller to think about the situation in which the call is being made and not forget to include an error if there is one.
        """
        if error and not isinstance(error, Exception):
            raise ValueError("Error parameter is not of legal type")
        if not self._wrapped_publisher:
            raise RuntimeError("StatusBufferPublisher not context managed")
        logger.debug("%s: Sending Complete, with error '%s'", self._name, safe_str(error))
        self._wrapped_publisher.publish_complete(error, TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.LOG_WARNING)  # Don't raise if full, errors will build up


class StatusBufferSubscription(ContextManager["StatusBufferSubscription"], ScopedAttributesMixin):
    _wrapped_buffer: StatusMessageBuffer = copied("_wrapped_buffer")
    _parent: StatusBuffer = copied("_parent")
    _name: str = copied("_name")
    _wrapped_subscription: Optional[Subscription[StatusMessage]] = python_default("_wrapped_subscription")
    _running: bool = copied("_running")
    _finished: bool = copied("_finished")
    _exception: Optional[TraceableException] = copied("_exception")
    _event: AutoResetEvent = manually_managed("_event", ThreadAction.SHARED, ProcessAction.SET_TO_NONE)
    _exceptions_lock: threading.Lock = manually_managed("_exceptions_lock", ThreadAction.SHARED, ProcessAction.SET_TO_NONE)
    _subscription: Optional[Subscription] = copied("_subscription")
    _status_cache: Dict[str, StatusMessage] = copied("_status_cache")

    def __init__(self, wrapped_buffer: StatusMessageBuffer, parent: StatusBuffer) -> None:
        super().__init__()
        self._wrapped_buffer = wrapped_buffer
        self._parent = parent
        self._name = wrapped_buffer.buffer_name()
        self._wrapped_subscription = per_scope_value(None)
        self._running = False  # Set when we are told that run() has started and false when self._finished is set
        self._finished = False  # Set when we are told of a fatal exception or that run() has finished
        self._exception = None  # Set when we are told of a fatal exception
        self._event = AutoResetEvent()
        self._exceptions_lock = threading.Lock()
        self._subscription = per_scope_value(None)
        self._status_cache = factory(dict)  # Cache of the latest of each type of status message

    def __enter__(self) -> 'StatusBufferSubscription':
        logger.debug("%s: StatusBufferSubscription Entering context management", self._name)
        if self._wrapped_subscription:
            raise RuntimeError("StatusBufferSubscription already context managed")
        self._wrapped_subscription = self._wrapped_buffer.subscribe(self._event).__enter__()
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        unrolling_after_exception: bool = exc_type is not None
        logger.debug("%s: StatusBufferSubscription Exiting context management; unrolling after exception: %s", self._name, str(unrolling_after_exception))
        if not self._wrapped_subscription:
            raise RuntimeError("StatusBufferSubscription not context managed")
        try:
            if not unrolling_after_exception:
                self.check_for_exceptions()
        finally:
            self._wrapped_subscription.__exit__(exc_type, exc_value, traceback)
            self._wrapped_subscription = None

    def get_latest_status_message(self, status_message_type: Type[StatusMessageType]) -> Optional[StatusMessageType]:
        """ Called via a Runnable, returns the latest instance of the StatusMessage that matches the given type.

        Will be None if no messages of that type have yet been retrieved
        """
        try:
            return self.wait_for_status_message(status_message_type, TIMEOUT_NO_WAIT)
        except TimeoutError:
            return None

    def wait_for_status_message(self, status_message_type: Type[StatusMessageType], timeout: float = TIMEOUT_INFINITE) -> StatusMessageType:
        """Called via a Runnable, blocks until a status message of the type given is received, or until Complete has been received or a timeout expires.

        Returns the latest instance of the StatusMessage that matches the given type, or raises a TimeoutError if no new message of that type has been received within the timeout.
        Once a message of a given type has been returned to the caller, it is deleted and will not be returned again.
        """
        if not status_message_type:
            raise ValueError("Status message type must be provided")
        Timeouts.validate(timeout)
        logger.debug("%s: Waiting for Status Message, timeout is %s", self._name, Timeouts.describe(timeout))

        self._pop_queue_until_empty()

        message_cache_key = self._get_status_message_cache_key(status_message_type)

        max_wait_time_reached = True  # Set to True here to return a sensible message with NO_WAIT timeout
        if Timeouts.is_blocking(timeout):
            max_wait_time_reached = False
            end_time = Timeouts.end_time(time.monotonic(), timeout)
            while not self._finished and message_cache_key not in self._status_cache:
                remaining = end_time - time.monotonic()
                if remaining <= 0.0:
                    max_wait_time_reached = True
                    break
                self._event.wait(remaining)
                self._pop_queue_until_empty()
                self._raise_received_exception()

        # One more time, found to be helpful during testing
        self._pop_queue_until_empty()
        self._raise_received_exception()

        message = self._status_cache.pop(message_cache_key, None)  # Get and then forget the value once it's been returned to the caller
        if message:
            return cast(StatusMessageType, message)
        else:
            error_msg_start = f"Unable to retrieve status message {status_message_type}"
            if max_wait_time_reached:
                raise TimeoutError(f"{error_msg_start} within timeout: {Timeouts.describe(timeout)} seconds")
            else:
                raise TimeoutError(f"{error_msg_start} as the Runnable has ended without publishing a matching message")

    def block_until_running(self, timeout: float = TIMEOUT_INFINITE) -> bool:
        """Called by the owner of a Runner, blocks until the Runner has started (or has ended without error), or until a timeout expires.

        Returns true if the Runner started (or has already finished without error), False if the timeout expired.
        Raises an exception if the Runner pushed an error onto the queue.
        """
        Timeouts.validate(timeout)
        if self._running:
            logger.debug("%s: block_until_running returning immediately, already found to be running", self._name)
            return True

        logger.debug("%s: block_until_running waiting for StartedStatusMessage, timeout is %s", self._name, Timeouts.describe(timeout))
        try:
            self.wait_for_status_message(StartedStatusMessage, timeout)
            self._running = True
        except TimeoutError:
            self._running = False

        logger.debug("%s: block_until_running returning %s", self._name, str(self._running))
        return self._running

    def _pop_queue_until_empty(self) -> None:
        if not self._wrapped_subscription:
            raise RuntimeError("StatusBufferSubscription not context managed")
        while True:
            try:
                self._wrapped_subscription.call_events(self._on_value, self._on_complete)
            except queue.Empty:
                return

    def check_for_exceptions(self) -> None:
        """Called by the owner of a Runner, pops status messages from the queue, and if any of them are errors then raises the error."""
        logger.debug("%s: Checking for exceptions", self._name)
        self._pop_queue_until_empty()
        self._raise_received_exception()

    def _raise_received_exception(self) -> None:
        with self._exceptions_lock:
            if self._exception:
                logger.debug("%s: Checking for exceptions: Raising exception '%s'", self._name, safe_str(self._exception))
                # don't raise the same exception more than once
                ret: TraceableException = self._exception
                self._exception = None
                raise ret.get_error()

    def _on_value(self, value: StatusMessage) -> None:
        """Called when the owning thread/process receive a status message from the Runner."""
        logger.debug("%s: Received value %s", self._name, str(value))
        self._status_cache[self._get_status_message_cache_key(value)] = value

    def _on_complete(self, error: Optional[BaseException]) -> None:
        """Called when the owning thread/process receive a 'complete' message from the Runner."""
        logger.debug("%s: Received Complete, with error '%s'", self._name, safe_str(error))
        self._finished = True
        if error:
            with self._exceptions_lock:
                if self._exception:
                    logger.warning("%s: Ignoring received error because a previous error has not yet been raised", self._name)
                else:
                    self._exception = TraceableException(error)

    def _get_status_message_cache_key(self, status_message_type_or_instance: Union[StatusMessage, Type[StatusMessage]]) -> str:
        if isinstance(status_message_type_or_instance, RunInChildScopeStatusMessage):
            return status_message_type_or_instance.call_id

        if isinstance(status_message_type_or_instance, StatusMessage):
            status_message_type_or_instance = status_message_type_or_instance.__class__

        if issubclass(status_message_type_or_instance, StatusMessage):
            return status_message_type_or_instance.__name__
        else:
            raise RuntimeError(f"Unable to determine StatusMessage key for given message type or instance: {status_message_type_or_instance}")
