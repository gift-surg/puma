from typing import Optional

from puma.attribute import copied
from puma.context import Exit_1, Exit_2, Exit_3, ensure_used_within_context_manager
from puma.helpers.assert_set import assert_set
from puma.runnable.message import StatusBuffer, StatusBufferSubscription
from puma.runnable.remote_execution import RemoteObjectStatusBufferSubscriptionManager


class ProxyRemoteObjectStatusBufferSubscriptionManager(RemoteObjectStatusBufferSubscriptionManager):
    _buffer: StatusBuffer = copied("_buffer")
    _subscription: Optional[StatusBufferSubscription] = copied("_subscription")

    def __init__(self, buffer: StatusBuffer) -> None:
        super().__init__()
        self._buffer = buffer
        self._subscription = None

    def __enter__(self) -> RemoteObjectStatusBufferSubscriptionManager:
        self._subscription = self._buffer.subscribe()
        self._subscription.__enter__()
        return super().__enter__()

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        assert_set(self._subscription).__exit__(exc_type, exc_value, traceback)
        self._subscription = None
        super().__exit__(exc_type, exc_value, traceback)

    @property  # type: ignore
    @ensure_used_within_context_manager
    def subscription(self) -> StatusBufferSubscription:
        return assert_set(self._subscription)
