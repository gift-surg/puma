from abc import ABC, abstractmethod
from typing import Generic, TypeVar

from puma.attribute.attribute.detected_scope import DetectedScope
from puma.attribute.mixin import ScopedAttributesMixin

T = TypeVar("T")


class ScopedAttributeValueFactory(Generic[T], ScopedAttributesMixin, ABC):
    """Factory that returns scope-specific values for an attribute"""

    @abstractmethod
    def get_value(self, detected_scope: DetectedScope) -> T:
        """Returns or creates the attribute's value for the given scope (parent or child)"""
        raise NotImplementedError()
