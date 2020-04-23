from abc import ABC, abstractmethod

from puma.attribute.mixin import ScopedAttributesMixin
from puma.context import ContextManager, Exit_1, Exit_2, Exit_3, ensure_used_within_context_manager, must_be_context_managed
from puma.runnable.message import StatusBufferSubscription


@must_be_context_managed
class RemoteObjectStatusBufferSubscriptionManager(ContextManager["RemoteObjectStatusBufferSubscriptionManager"], ScopedAttributesMixin, ABC):

    def __enter__(self) -> "RemoteObjectStatusBufferSubscriptionManager":
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        # Do nothing
        pass

    @property  # type: ignore
    @ensure_used_within_context_manager
    @abstractmethod
    def subscription(self) -> StatusBufferSubscription:
        raise NotImplementedError()
