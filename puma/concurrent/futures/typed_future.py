from concurrent.futures import Future
from typing import Generic, TYPE_CHECKING, TypeVar

# This file has slightly odd formatting to prevent PyCharm complaining

T = TypeVar("T")

if TYPE_CHECKING:
    # noinspection PyUnresolvedReferences
    class _InternalFuture(Generic[T], Future[T]):
        pass
else:
    class _InternalFuture(Generic[T], Future):
        pass


# noinspection PyUnresolvedReferences
class TypedFuture(Generic[T], _InternalFuture[T]):
    pass
