from typing import Any, Dict

from puma.attribute import ATTRIBUTE_NAME_PREFIX
from puma.attribute.attribute.scoped_attribute import ScopedAttribute
from puma.attribute.mixin import ScopedAttributesBaseMixin
from puma.attribute.mixin.scoped_attributes_base_mixin import PRIVATE_ATTRIBUTE_PREFIX

SPECIAL_ATTRS = [
    "_is_within_context"  # Used for ContextManagement (puma.context)
]


class ScopedAttributesMixin(ScopedAttributesBaseMixin):
    """
    This Mixin applies __getstate__, __setstate__ and __getattribute__ methods that ensure attributes are properly defined so that they work correctly and consistently
    when being shared across Threads and Processes
    """

    def __setattr__(self, key: str, value: Any) -> None:
        # Allow ScopedAttributes internal attributes and private attributes ("__" prefix) to be stored without further checks
        if key.startswith(ATTRIBUTE_NAME_PREFIX) or key.startswith(PRIVATE_ATTRIBUTE_PREFIX) or key in SPECIAL_ATTRS:
            self.__dict__[key] = value
            return

        # Check that the attribute exists at a class level as this is where the attribute MUST be defined
        scoped_attribute: ScopedAttribute = getattr(self.__class__, key, None)
        if scoped_attribute:
            scoped_attribute.__set__(self, value)
        else:
            raise InvalidAttributeTypeError(key)

    def __getattribute__(self, item: str) -> Any:
        """Custom __getattribute__ method used to prevent improperly defined instance properties from being accessible"""
        self_dict = super().__getattribute__("__dict__")
        value = super().__getattribute__(item)

        # TODO: Production mode that skips checks??

        if item in SPECIAL_ATTRS:
            return value

        # Detect non-ScopedAttributes and raise an error when attempting to access them
        if not item.startswith(ATTRIBUTE_NAME_PREFIX) and item in self_dict:
            raise InvalidAttributeTypeError(item)
        return value

    def _handle_non_scoped_attribute_item(self, attributes: Dict[str, Any], key: str) -> None:
        # Set the attribute to "None" so that it still appears in the object's __dict__ but it can't cause a problem with unpickleable values
        attributes[key] = None


class InvalidAttributeTypeError(ValueError):

    def __init__(self, name: str) -> None:
        super().__init__(f"Using non-ScopedAttributes with {ScopedAttributesMixin.__name__} is not allowed. "
                         f"Please define the variable '{name}' at the class level using any of parent_only, child_only or copied")
