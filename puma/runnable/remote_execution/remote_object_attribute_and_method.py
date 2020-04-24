import inspect
import logging
from abc import ABC, abstractmethod
from time import monotonic
from typing import Any, Callable, Generic, Optional, TypeVar, cast
from uuid import uuid4

from puma.buffer import Buffer
from puma.runnable.message import RemoteObjectGetAttributeCommandMessage, RemoteObjectMethodCommandMessage, RunInChildScopeCommandMessage, RunInChildScopeStatusMessage, \
    status_message_type
from puma.runnable.remote_execution import RemoteObjectResultReference, RemoteObjectStatusBufferSubscriptionManager
from puma.runnable.remote_execution.timestamp_tracker import TimestampTracker

MethodType = TypeVar("MethodType", bound=Callable[..., Any])
R = TypeVar("R")

REMOTE_METHOD_CALL_DEFAULT_TIMEOUT = 60.

CALL_CHECK_HISTORY_LENGTH = 10
CALL_CHECK_RATE_LOOKBACK_SECONDS = 1
CALL_CHECK_RATE_LOOKBACK_COUNT = 3
CALL_CHECK_LIMIT_RESPONSE_SECONDS = 1

logger = logging.getLogger(__name__)


class _AbstractRemoteObjectAttribute(ABC):

    def __init__(self, result_object_id: Optional[str], attribute_or_method_name: str, remote_command_buffer: Buffer[RunInChildScopeCommandMessage],
                 remote_status_buffer_subscription_manager: RemoteObjectStatusBufferSubscriptionManager, call_timeout_seconds: float = REMOTE_METHOD_CALL_DEFAULT_TIMEOUT) -> None:
        self._result_object_id = result_object_id
        self._attribute_or_method_name = attribute_or_method_name
        self._remote_command_buffer = remote_command_buffer
        self._remote_status_buffer_subscription_manager = remote_status_buffer_subscription_manager
        self._call_timeout_seconds = call_timeout_seconds
        self._call_check_timestamps = TimestampTracker(CALL_CHECK_HISTORY_LENGTH)
        self._call_check_warning_shown = False

    def __call__(self, instance_self_unused: Any, *args: Any, **kwargs: Any) -> Any:
        return self._call_and_wait_for_result(*[instance_self_unused, *args], **kwargs)

    def _call_and_wait_for_result(self, *args: Any, **kwargs: Any) -> R:

        call_start_time = monotonic()

        return_value_id = self._generate_return_value_id()
        control_message = self._create_command_message(return_value_id, args, kwargs, self._result_object_id)
        with self._remote_command_buffer.publish() as publisher:
            publisher.publish_value(control_message)
        return_message_type = status_message_type(return_value_id, RunInChildScopeStatusMessage)

        with self._remote_status_buffer_subscription_manager as subscription_manager:
            message: RunInChildScopeStatusMessage[R] = subscription_manager.subscription.wait_for_status_message(return_message_type, self._get_remote_call_timeout())

        # Check for sub optimal usage; a) high rate of usage or b) slow response
        # Only check if a warning hasn't already been shown so as to not flood the log
        if not self._call_check_warning_shown:
            call_duration = monotonic() - call_start_time
            if call_duration > CALL_CHECK_LIMIT_RESPONSE_SECONDS:
                # Show warning
                logger.warning(f"Slow response when calling {self.__class__.__name__} '{self._attribute_or_method_name}' - took {call_duration} seconds. "
                               f"Please check for a slow _on_tick or on_value method")
                self._call_check_warning_shown = True
            else:
                # Record latest call and check call rate
                self._call_check_timestamps.record(call_start_time)
                call_count = len(self._call_check_timestamps.entries_in_last_n_seconds(CALL_CHECK_RATE_LOOKBACK_SECONDS))
                if call_count > CALL_CHECK_RATE_LOOKBACK_COUNT:
                    # Show warning
                    logger.warning(f"Excessive calling of {self.__class__.__name__} '{self._attribute_or_method_name}' - "
                                   f"{call_count} in less than {CALL_CHECK_RATE_LOOKBACK_SECONDS} second(s) (max allowed = {CALL_CHECK_RATE_LOOKBACK_COUNT})")
                    self._call_check_warning_shown = True

        result = message.result
        if isinstance(result, RemoteObjectResultReference):
            return cast(R, result.with_attached_remote_buffers(self._remote_command_buffer, self._remote_status_buffer_subscription_manager))
        else:
            return result

    @abstractmethod
    def _create_command_message(self, return_value_id: str, args: Any, kwargs: Any, result_object_id: Optional[str]) -> RunInChildScopeCommandMessage:
        raise NotImplementedError()

    def _generate_return_value_id(self) -> str:
        return str(uuid4())

    def _get_remote_call_timeout(self) -> float:
        return self._call_timeout_seconds


class RemoteObjectMethod(Generic[MethodType], _AbstractRemoteObjectAttribute):

    def __init__(self, result_object_id: Optional[str], attribute_or_method_name: MethodType, remote_command_buffer: Buffer[RunInChildScopeCommandMessage],
                 remote_status_buffer_subscription_manager: RemoteObjectStatusBufferSubscriptionManager, call_timeout_seconds: float = REMOTE_METHOD_CALL_DEFAULT_TIMEOUT) -> None:
        super().__init__(result_object_id, attribute_or_method_name.__name__, remote_command_buffer, remote_status_buffer_subscription_manager, call_timeout_seconds)
        self._method = attribute_or_method_name

        self.call = cast(MethodType, self._call_and_wait_for_result)

    def _create_command_message(self, return_value_id: str, args: Any, kwargs: Any, result_object_id: Optional[str]) -> RunInChildScopeCommandMessage:
        return RemoteObjectMethodCommandMessage(return_value_id, self._attribute_or_method_name, result_object_id, args, kwargs)

    def _call_and_wait_for_result(self, *args: Any, **kwargs: Any) -> R:

        if len(args) > 0:
            # Replace AutoRemoteObjectReference with a placeholder to prevent it being pickled when being passed to the child process
            from puma.runnable.remote_execution import AutoRemoteObjectReference
            if isinstance(args[0], AutoRemoteObjectReference):
                args = (AutoRemoteObjectReferencePlaceholder(), *args[1:])

        self._verify_signature(*args, *kwargs)

        return super()._call_and_wait_for_result(*args, **kwargs)

    def _verify_signature(self, *args: Any, **kwargs: Any) -> None:
        # Ensure the correct arguments have been given
        signature = inspect.signature(self._method)
        signature.bind(*args, **kwargs)


class RemoteObjectAttribute(_AbstractRemoteObjectAttribute):

    def _create_command_message(self, return_value_id: str, args: Any, kwargs: Any, result_object_id: Optional[str]) -> RunInChildScopeCommandMessage:
        return RemoteObjectGetAttributeCommandMessage(return_value_id, self._attribute_or_method_name, result_object_id)

    def get_value(self) -> Any:
        return self._call_and_wait_for_result()


class AutoRemoteObjectReferencePlaceholder:
    pass
