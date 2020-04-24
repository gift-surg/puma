from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar

from puma.attribute.attribute.detected_scope import DetectedScope
from puma.attribute.attribute.scoped_attribute_value_factory import ScopedAttributeValueFactory
from puma.attribute.attribute_proxy import AttributeProxy, proxy, shared_within_instance_proxy

T = TypeVar("T")


class InstanceWrapper(Generic[T], ABC):
    """Encapsulates the several sub-attributes needed to determine the value of an attribute"""
    value: AttributeProxy[T] = proxy()
    value_factory: AttributeProxy[Optional[ScopedAttributeValueFactory[T]]] = proxy()
    value_factory_called: AttributeProxy[bool] = proxy()
    parent_scope_id: AttributeProxy[str] = shared_within_instance_proxy()

    @abstractmethod
    def detect_scope(self) -> DetectedScope:
        raise NotImplementedError()
