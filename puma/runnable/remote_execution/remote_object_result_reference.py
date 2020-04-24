from typing import Any

from puma.buffer import Buffer
from puma.runnable.message import CommandMessage
from puma.runnable.remote_execution import RemoteObjectStatusBufferSubscriptionManager


class RemoteObjectResultReference:
    """Stores a reference to the non-primitive result of a remotely called method. Allows method calls against the result to be properly called back to the original scope"""

    def __init__(self, return_value_id: str, result: Any):
        self._return_value_id = return_value_id
        self._result_type = result.__class__

    def with_attached_remote_buffers(self, remote_command_buffer: Buffer[CommandMessage],
                                     remote_status_buffer_subscription_manager: RemoteObjectStatusBufferSubscriptionManager) -> Any:
        from puma.runnable.remote_execution import AutoRemoteObjectReference
        return AutoRemoteObjectReference(self._result_type, self._return_value_id, remote_command_buffer, remote_status_buffer_subscription_manager)
