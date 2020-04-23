from typing import Any, Generic, Optional, TypeVar, Union, cast

from puma.attribute import ATTRIBUTE_NAME_SEPARATOR
from puma.attribute.attribute.value_not_found_handler.raise_error_value_not_found_handler import RaiseErrorValueNotFoundHandler
from puma.attribute.attribute_proxy.attribute_proxy import AttributeProxy, ValueNotFoundHandler
from puma.attribute.scope_id_helpers import format_scope_id_for_attribute_id

T = TypeVar("T")


class AttributeProxyImpl(Generic[T], AttributeProxy[T]):

    def __init__(self, instance: Any, fully_qualified_name: str, parent_attribute_name: str, current_scope_id: str) -> None:
        self._instance = instance
        self._fully_qualified_name = fully_qualified_name
        self._parent_attribute_name = parent_attribute_name
        self._current_scope_id = current_scope_id

    @property
    def fully_qualified_name(self) -> str:
        return self._fully_qualified_name

    @property
    def parent_attribute_name(self) -> str:
        return self._parent_attribute_name

    @property
    def instance(self) -> Any:
        return self._instance

    def storage_key(self, scope_id: Optional[str] = None) -> str:
        if scope_id is None:
            scope_id = self._current_scope_id
        return f"{self.fully_qualified_name}{format_scope_id_for_attribute_id(scope_id)}"

    def get(self, default_value_or_accessor: Optional[Union[T, ValueNotFoundHandler[T]]] = None) -> T:

        if default_value_or_accessor is None:
            default_value_or_accessor = RaiseErrorValueNotFoundHandler()

        var_name = self.storage_key()
        if hasattr(self.instance, var_name):
            return cast(T, getattr(self.instance, var_name))
        else:
            if isinstance(default_value_or_accessor, ValueNotFoundHandler):
                return default_value_or_accessor.get_value(self)
            else:
                return default_value_or_accessor

    def set(self, value: T) -> None:
        setattr(self.instance, self.storage_key(), value)


class SharedWithinInstanceAttributeProxyImpl(AttributeProxyImpl):
    """An AttributeProxyImpl that ensures that all attributes share the same value within an instance"""

    def __init__(self, instance: Any, fully_qualified_name: str, parent_attribute_name: str, current_scope_id: str) -> None:
        # Get name components
        name_components = fully_qualified_name.split(ATTRIBUTE_NAME_SEPARATOR)
        # Remove the middle component (object id and specific attribute name)
        name_components.pop(1)
        # Recombine
        new_name = "".join(name_components).replace("__", "_")
        super().__init__(instance, new_name, parent_attribute_name, current_scope_id)
