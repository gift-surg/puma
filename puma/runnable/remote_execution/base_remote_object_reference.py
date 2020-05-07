from typing import Any, Callable, Generic, TypeVar, cast

from puma.attribute import copied, scope_specific
from puma.attribute.mixin import ScopedAttributesCompatibilityMixin
from puma.buffer import Buffer
from puma.runnable.remote_execution import RemoteObjectMethod, RemoteObjectStatusBufferSubscriptionManager
from puma.runnable.remote_execution.remote_object_attribute_and_method import REMOTE_METHOD_CALL_DEFAULT_TIMEOUT

T = TypeVar("T")
MethodType = TypeVar("MethodType", bound=Callable[..., Any])


class BaseRemoteObjectReference(Generic[T], ScopedAttributesCompatibilityMixin):
    _wrapped_instance: T = copied("_wrapped_instance")
    _remote_object_command_buffer: Buffer = copied("_remote_object_command_buffer")
    _remote_object_status_buffer_subscription_manager: RemoteObjectStatusBufferSubscriptionManager = copied("_remote_object_status_buffer_subscription_manager")

    def __init__(self, wrapped: T, remote_object_command_buffer: Buffer, remote_object_status_buffer_subscription_manager: RemoteObjectStatusBufferSubscriptionManager) -> None:
        super().__init__()
        self._wrapped_instance: T = scope_specific(wrapped, self._get_wrapper)
        self._remote_object_command_buffer = remote_object_command_buffer
        self._remote_object_status_buffer_subscription_manager = remote_object_status_buffer_subscription_manager

    def _get_wrapper(self) -> T:
        return cast(T, self)

    def _remote_method(self, local_method: MethodType, call_timeout_seconds: float = REMOTE_METHOD_CALL_DEFAULT_TIMEOUT) -> RemoteObjectMethod[MethodType]:
        return RemoteObjectMethod(None, local_method, self._remote_object_command_buffer, self._remote_object_status_buffer_subscription_manager, call_timeout_seconds)
