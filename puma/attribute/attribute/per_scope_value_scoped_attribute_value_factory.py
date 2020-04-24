from typing import Generic, TypeVar

from puma.attribute import copied
from puma.attribute.attribute.scoped_attribute_value_factory_impl import ScopedAttributeValueFactoryImpl

T = TypeVar("T")


class PerScopeValueScopedAttributeValueFactory(Generic[T], ScopedAttributeValueFactoryImpl[T]):
    """An ScopedAttributeValueFactory that resets the attribute to the given value at the start of each scope"""
    _value: T = copied("_value")

    def __init__(self, value: T):
        super().__init__(self._static_value)
        self._value = value

    def _static_value(self) -> T:
        return self._value
