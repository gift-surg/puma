from typing import Callable, Optional, TypeVar

from puma.attribute import child_only, child_scope_value, copied, unmanaged
from puma.attribute.mixin import ScopedAttributesMixin
from puma.buffer import DEFAULT_PUBLISH_COMPLETE_TIMEOUT, DEFAULT_PUBLISH_VALUE_TIMEOUT, Publishable, Publisher
from puma.context import Exit_1, Exit_2, Exit_3
from puma.primitives import AutoResetEvent
from puma.unexpected_situation_action import UnexpectedSituationAction

PType = TypeVar("PType")


class _InRunnableIndirectPublisher(Publisher[PType], ScopedAttributesMixin):
    """Used in a Runnable, this implements the Publisher interface, passing on its calls to an actual Publisher which is set when the runnable executes.

    Before the runnable has executed, it is only legal to get the name of the publisher; other calls will fail.
    """
    _id: int = copied("_id")
    _publishable: Publishable = copied("_publishable")
    _publisher: Optional[Publisher] = child_only("_publisher")
    _publisher_accessor: Callable[[int], Publisher] = unmanaged("_publisher_accessor")

    def __init__(self, publishable: Publishable, publisher_accessor: Callable[[int], Publisher]) -> None:
        super().__init__()
        self._id = hash(publishable)
        self._publishable = publishable
        self._publisher = child_scope_value(None)
        # MyPy complains about assigning to a method: https://github.com/python/mypy/issues/708
        self._publisher_accessor = publisher_accessor  # type: ignore

    def set_publisher(self, publisher: Optional[Publisher[PType]]) -> None:
        self._publisher = publisher

    def __enter__(self) -> 'Publisher[PType]':
        if self._get_publisher() is None:
            raise RuntimeError("Trying to enter context management of a publisher that has not been set up")
        self._get_publisher().__enter__()
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        if self._get_publisher() is None:
            raise RuntimeError("Trying to exit context management of a publisher that has not been set up")
        self._get_publisher().__exit__(exc_type, exc_value, traceback)

    def publish_value(self, value: PType, timeout: float = DEFAULT_PUBLISH_VALUE_TIMEOUT,
                      on_full_action: UnexpectedSituationAction = UnexpectedSituationAction.RAISE_EXCEPTION) -> None:
        self._get_publisher().publish_value(value, timeout, on_full_action)

    def publish_complete(self, error: Optional[BaseException], timeout: float = DEFAULT_PUBLISH_COMPLETE_TIMEOUT,
                         on_full_action: UnexpectedSituationAction = UnexpectedSituationAction.RAISE_EXCEPTION) -> None:
        self._get_publisher().publish_complete(error, timeout, on_full_action)

    def buffer_name(self) -> str:
        return self._publishable.buffer_name()

    def invalidate(self) -> None:
        self._get_publisher().invalidate()

    def set_subscriber_event(self, subscriber_event: Optional[AutoResetEvent]) -> None:
        self._get_publisher().set_subscriber_event(subscriber_event)

    def _get_publisher(self) -> Publisher[PType]:
        if self._publisher is None:
            # Attempt to retrieve it once via _publisher_accessor for cross scope usage
            # MyPy again complains erroneously: https://github.com/python/mypy/issues/708
            self._publisher = self._publisher_accessor(self._id)  # type: ignore

        return self._publisher
