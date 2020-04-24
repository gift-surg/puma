from typing import Any, Dict

from puma.attribute.mixin import ScopedAttributesBaseMixin

KEY_PARENT = "parent"


class ScopedAttributesCompatibilityMixin(ScopedAttributesBaseMixin):

    def _handle_non_scoped_attribute_item(self, attributes: Dict[str, Any], key: str) -> None:
        # Do nothing
        pass
