from typing import Any, Generic, Type, TypeVar, cast

from puma.attribute import ProcessAction, ThreadAction, copied, manually_managed, parent_only, unmanaged
from puma.buffer import Buffer, MultiProcessBuffer
from puma.buffer.default_subscriber import DefaultSubscriber
from puma.context import ContextManager, Exit_1, Exit_2, Exit_3, ensure_used_within_context_manager, must_be_context_managed
from puma.runnable import MultiBufferServicingRunnable
from puma.runnable.message import RemoteObjectMethodCommandMessage, StatusBuffer, StatusMessage
from puma.runnable.remote_execution import AutoRemoteObjectReference, ProxyRemoteObjectStatusBufferSubscriptionManager
from puma.runnable.runner import ThreadRunner

T = TypeVar("T")
PROXY_BUFFER_SIZE = 10


@must_be_context_managed
class Proxy(Generic[T], MultiBufferServicingRunnable, ContextManager["Proxy[T]"]):
    _interface: Type = copied("_interface")
    _wrapped: Any = manually_managed("_wrapped", ThreadAction.SHARED, ProcessAction.NOT_ALLOWED)
    _proxy_command_buffer: Buffer[RemoteObjectMethodCommandMessage] = copied("_proxy_command_buffer")
    _wrapped_proxy_status_buffer: Buffer[StatusMessage] = parent_only("_wrapped_proxy_status_buffer")
    _proxy_status_buffer: StatusBuffer = unmanaged("_proxy_status_buffer")

    def __init__(self, interface: Type, wrapped: T) -> None:
        super().__init__(f"{self.__class__.__name__} for {wrapped}", [])
        self._interface = interface
        self._wrapped = wrapped
        self._proxy_command_buffer = MultiProcessBuffer(PROXY_BUFFER_SIZE, f"Proxy Command buffer for {self.get_name()}")
        self._wrapped_proxy_status_buffer = MultiProcessBuffer(PROXY_BUFFER_SIZE, f"Proxy Status buffer for {self.get_name()}")
        self._proxy_status_buffer = StatusBuffer(self._wrapped_proxy_status_buffer)

        self._add_subscription(self._proxy_command_buffer, DefaultSubscriber(self.on_value))

    def __enter__(self) -> "Proxy[T]":
        self._proxy_command_buffer.__enter__()
        self._wrapped_proxy_status_buffer.__enter__()
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        self._wrapped_proxy_status_buffer.__exit__(exc_type, exc_value, traceback)
        self._proxy_command_buffer.__exit__(exc_type, exc_value, traceback)

    @ensure_used_within_context_manager
    def get_runner(self) -> ThreadRunner:
        return ThreadRunner(self, f"Proxy ThreadRunner for {self._wrapped}")

    def on_value(self, command: RemoteObjectMethodCommandMessage) -> None:
        with self._proxy_status_buffer.publish() as status_publisher:
            try:
                self._handle_run_in_child_scope_command(command, status_publisher)
            except Exception as e:
                status_publisher.publish_complete(e)

    def _retrieve_remote_attribute_or_method(self, name: str) -> Any:
        return getattr(self._wrapped, name)

    @ensure_used_within_context_manager
    def get_facade(self) -> T:
        return cast(T, AutoRemoteObjectReference(self._interface, self._wrapped, self._proxy_command_buffer,
                                                 ProxyRemoteObjectStatusBufferSubscriptionManager(self._proxy_status_buffer)))
