from copy import deepcopy
from dataclasses import dataclass
from typing import Any

from puma.attribute import SCOPE_ID_DELIMITER
from puma.attribute.attribute_proxy import AttributeProxy
from puma.attribute.attribute_proxy.attribute_proxy import ValueNotFoundHandler, can_be_shared_without_copying
from puma.helpers.class_name import get_fully_qualified_name


@dataclass()
class SearchStatus:
    found: bool
    value: Any


class UseParentValueNotFoundHandler(ValueNotFoundHandler[Any]):
    """A ValueNotFoundHandler that returns the attribute's value from its parent scope, or None"""

    def __init__(self, parent_scope_id: str) -> None:
        self._parent_scope_id = parent_scope_id

    def get_value(self, attribute_proxy_instance: AttributeProxy) -> Any:

        search_status = SearchStatus(False, None)

        # Fast look up
        fast_lookup_key = attribute_proxy_instance.storage_key(self._parent_scope_id)
        if fast_lookup_key in attribute_proxy_instance.instance.__dict__:
            search_status = SearchStatus(True, attribute_proxy_instance.instance.__dict__[fast_lookup_key])
        else:
            # Fallback (slower) lookup method
            var_name = attribute_proxy_instance.storage_key()
            for name in list(attribute_proxy_instance.instance.__dict__.keys()):
                value = attribute_proxy_instance.instance.__dict__[name]
                if name.startswith(f"{attribute_proxy_instance.fully_qualified_name}{SCOPE_ID_DELIMITER}") and name != var_name:
                    search_status = SearchStatus(True, value)
                    break

        if search_status.found:
            parent_value = search_status.value
            if can_be_shared_without_copying(parent_value):
                attribute_proxy_instance.set(parent_value)
                return parent_value
            else:
                try:
                    copied_value = deepcopy(parent_value)
                    attribute_proxy_instance.set(copied_value)
                    return copied_value
                except Exception as e:
                    raise RuntimeError(f"Unable to create copy of attribute '{attribute_proxy_instance.parent_attribute_name}' "
                                       f"belonging to '{get_fully_qualified_name(attribute_proxy_instance.instance)}'") from e
        else:
            return None
