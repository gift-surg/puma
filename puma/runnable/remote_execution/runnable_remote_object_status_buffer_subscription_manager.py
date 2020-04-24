from puma.attribute import copied
from puma.context import ensure_used_within_context_manager
from puma.runnable.message import StatusBufferSubscription
from puma.runnable.remote_execution import RemoteObjectStatusBufferSubscriptionManager


class RunnableRemoteObjectStatusBufferSubscriptionManager(RemoteObjectStatusBufferSubscriptionManager):
    _runnable_status_buffer_subscription: StatusBufferSubscription = copied("_runnable_status_buffer_subscription")

    def __init__(self, runnable_status_buffer_subscription: StatusBufferSubscription) -> None:
        super().__init__()
        self._runnable_status_buffer_subscription = runnable_status_buffer_subscription

    @property  # type: ignore
    @ensure_used_within_context_manager
    def subscription(self) -> StatusBufferSubscription:
        return self._runnable_status_buffer_subscription
