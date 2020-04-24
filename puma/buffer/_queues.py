import multiprocessing
import queue
from multiprocessing import queues as multiprocessing_queues
from typing import Generic, TYPE_CHECKING, TypeVar, Union

from puma.attribute.mixin import ScopedAttributesCompatibilityMixin

# Queue Typing hack - see https://github.com/python/mypy/issues/5264 - because the runtime does not recognise the generic
# nature of Queue and raises an exception "'Queue' object is not subscriptable"
# NOTE: Client code should use ManagedThreadQueue and ManagedProcessQueue instead of these low-level queues.


if TYPE_CHECKING:
    _ThreadQueue = queue.Queue
    """Type definition of a Queue variable for use between threads."""

    _ProcessQueue = multiprocessing.Queue
    """Type definition of a Queue variable for use between processes."""
else:
    T = TypeVar("T")


    # noqa: E303
    class _ThreadQueue(Generic[T], queue.Queue, ScopedAttributesCompatibilityMixin):  # noqa: E301
        """Type definition of a Queue variable for use between threads."""

        def __getitem__(self, item: T) -> '_ThreadQueue[T]':
            return queue.Queue


    # noqa: E303
    class _ProcessQueue(Generic[T], multiprocessing_queues.Queue, ScopedAttributesCompatibilityMixin):  # noqa: E301
        """Type definition of a Queue variable for use between processes."""

        def __init__(self, maxsize: int = 0) -> None:
            super().__init__(maxsize, ctx=multiprocessing.get_context())

        def __getitem__(self, item: T) -> '_ProcessQueue[T]':
            return multiprocessing.Queue

TQ = TypeVar("TQ")
_QueueType = Union[_ThreadQueue[TQ], _ProcessQueue[TQ]]
