from typing import Generic, TypeVar

from puma.attribute import ProcessAction, ThreadAction, ValueFactory, manually_managed
from puma.attribute.attribute.detected_scope import DetectedScope
from puma.attribute.attribute.scoped_attribute_value_factory_impl import ScopedAttributeValueFactoryImpl
from puma.helpers.enum import EnumMemberError

T = TypeVar("T")


class ScopeSpecificScopedAttributeValueFactory(Generic[T], ScopedAttributeValueFactoryImpl[T]):
    """An ScopedAttributeValueFactory that returns a stored instance of an object in the parent scope and generates a new instance in the child scope"""
    _parent_value: T = manually_managed("_parent_value", ThreadAction.SHARED, ProcessAction.SET_TO_NONE)

    def __init__(self, parent_value: T, child_factory: ValueFactory[T]) -> None:
        super().__init__(child_factory)
        self._parent_value: T = parent_value

    def get_value(self, detected_scope: DetectedScope) -> T:
        if detected_scope == DetectedScope.child:
            # MyPy again complains erroneously: https://github.com/python/mypy/issues/708
            return self._value_factory()  # type: ignore
        elif detected_scope == DetectedScope.parent:
            return self._parent_value
        else:
            raise EnumMemberError(f"Invalid DetectedScope: {detected_scope}")
