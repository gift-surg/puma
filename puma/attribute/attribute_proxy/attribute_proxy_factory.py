from typing import Any, Generic, TypeVar

from puma.attribute.attribute.scoped_attribute import ScopedAttribute
from puma.attribute.attribute_proxy import AttributeProxy
from puma.attribute.attribute_proxy.attribute_proxy_impl import AttributeProxyImpl, SharedWithinInstanceAttributeProxyImpl
from puma.attribute.scope_id_helpers import create_attribute_id

T = TypeVar("T")


def _create_state_managed_id(state_manager: ScopedAttribute[T]) -> str:
    return state_manager.name or str(id(state_manager))


class AttributeProxyFactory(Generic[T]):
    """A factory of "AttributeProxy"s, allowing the AttributeProxy to be given the necessary instance, name and scope ID parameters"""

    def get_proxy(self, state_manager: ScopedAttribute[T], instance: Any, name: str, current_scope_id: str) -> AttributeProxy:
        state_manager_id = _create_state_managed_id(state_manager)
        return AttributeProxyImpl(instance, create_attribute_id(state_manager_id, name), state_manager_id, current_scope_id)


class SharedWithinInstanceAttributeProxyFactory(Generic[T], AttributeProxyFactory[T]):
    """A factory of "SharedWithinInstanceAttributeProxyImpl"s, allowing the AttributeProxy to be given the necessary instance, name and scope ID parameters"""

    def get_proxy(self, state_manager: ScopedAttribute[T], instance: Any, name: str, current_scope_id: str) -> AttributeProxy:
        state_manager_id = _create_state_managed_id(state_manager)
        return SharedWithinInstanceAttributeProxyImpl(instance, create_attribute_id(state_manager_id, name), state_manager_id, current_scope_id)
