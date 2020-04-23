from typing import Any

from puma.attribute.attribute_proxy import AttributeProxy
from puma.attribute.attribute_proxy.attribute_proxy import ValueNotFoundHandler


class RaiseErrorValueNotFoundHandler(ValueNotFoundHandler[Any]):
    """A ValueNotFoundHandler that raises an error indicating that the attribute's value could not be found"""

    def get_value(self, attribute_proxy_instance: AttributeProxy) -> Any:
        raise AttributeError(f"Unable to find attribute '{attribute_proxy_instance.fully_qualified_name}'")
