from typing import Any, Generic, Type, TypeVar, cast

from puma.attribute import ATTRIBUTE_NAME_PREFIX

T = TypeVar("T")

UNMANAGED_ATTRS = f"{ATTRIBUTE_NAME_PREFIX}__unmanaged_attrs__"


class UnmanagedScopedAttribute(Generic[T]):
    """
    A completely unmanaged class attribute which retains Python's default behaviour - copied between Process and shared between Threads

    This means that factory methods, such as "factory", "per_scope_value" etc. cannot be used
    """

    def __init__(self, name: str):
        self._name = name

    def __get__(self, instance: Any, owner: Type[Any]) -> T:
        if instance is None:
            return self  # type: ignore

        return cast(T, getattr(instance, UNMANAGED_ATTRS)[self._name])

    def __set__(self, instance: Any, value: T) -> None:
        if instance is None:
            return

        if not hasattr(instance, UNMANAGED_ATTRS):
            setattr(instance, UNMANAGED_ATTRS, {})

        getattr(instance, UNMANAGED_ATTRS)[self._name] = value
