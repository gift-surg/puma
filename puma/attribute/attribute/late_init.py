from typing import Any, Callable, Generic, Optional, Type, TypeVar, cast

T = TypeVar("T")


def late_init(value_method: Callable[[], T]) -> T:
    """ Creates a LateInitAttribute instance"""
    return cast(T, LateInitAttribute(value_method))


class LateInitAttribute(Generic[T]):
    """
    A class attribute whose value is not evaluated until it is first accessed.

    This may be useful if a defining a class attribute whose value is an instance of the same class; see DetectedScope for an example
    """

    def __init__(self, value_method: Callable[[], T]):
        self._value_method = value_method
        self._value: Optional[T] = None

    def __get__(self, instance: Any, owner: Type) -> T:
        if self._value is None:
            self._value = self._value_method()

        return self._value
