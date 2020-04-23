from typing import Any, Dict, Generic, Optional, TypeVar

from puma.attribute.attribute.detected_scope import DetectedScope
from puma.attribute.attribute.instance_wrapper import InstanceWrapper
from puma.attribute.attribute.scoped_attribute import SCOPE_ID_SHARED, ScopedAttribute
from puma.attribute.attribute.value_not_found_handler.use_parent_value_not_found_handler import UseParentValueNotFoundHandler
from puma.attribute.attribute_proxy import AttributeProxy, AttributeProxyFactory
from puma.scope_id import get_current_scope_id

T = TypeVar("T")

USE_PARENT = UseParentValueNotFoundHandler(get_current_scope_id())


class InstanceWrapperImpl(Generic[T], InstanceWrapper[T]):

    def __init__(self, state_manager: ScopedAttribute[T], instance: Any, current_scope_id: str) -> None:
        self._state_manager = state_manager
        self._instance = instance
        self._current_scope_id = current_scope_id
        self._proxy_caches: Dict[str, AttributeProxy] = {}
        self._detected_scope: Optional[DetectedScope] = None

        # If parent_scope_id hasn't been set that means that this is lives inside the parent scope
        if not self.parent_scope_id.get(USE_PARENT):
            self.parent_scope_id.set(current_scope_id)

    def __getattribute__(self, item: str) -> Any:
        value = super().__getattribute__(item)
        if isinstance(value, AttributeProxyFactory):
            cached_proxy = self._proxy_caches.get(item, None)
            if cached_proxy:
                return cached_proxy
            proxy = value.get_proxy(self._state_manager, self._instance, item, self._current_scope_id)
            self._proxy_caches[item] = proxy
            return proxy
        else:
            return value

    def detect_scope(self) -> DetectedScope:
        if not self._detected_scope:
            parent_scope_id = self.parent_scope_id.get()
            if parent_scope_id != self._current_scope_id and self._current_scope_id != SCOPE_ID_SHARED:
                self._detected_scope = DetectedScope.create_child_instance(self._current_scope_id)
            else:
                self._detected_scope = DetectedScope.create_parent_instance(parent_scope_id)

        return self._detected_scope
