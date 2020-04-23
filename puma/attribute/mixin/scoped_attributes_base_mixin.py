from abc import ABC, abstractmethod
from typing import Any, Dict, cast

from puma.attribute import ATTRIBUTE_NAME_PREFIX, AccessibleScope, ProcessAction, SCOPE_ID_PLACEHOLDER, SCOPE_ID_REGEX
from puma.attribute.attribute.scoped_attribute import ScopedAttribute
from puma.attribute.mixin import ScopedAttributeState
from puma.attribute.scope_id_helpers import format_scope_id_for_attribute_id
from puma.helpers.class_name import get_fully_qualified_name
from puma.mixin import Mixin
from puma.scope_id import get_current_scope_id

KEY_PARENT = "parent"
PRIVATE_ATTRIBUTE_PREFIX = "__"


class ScopedAttributesBaseMixin(Mixin, ABC):
    """
    This Mixin applies __getstate__ and __setstate__ methods that allow Objects with a mix of traditional attributes (ie, self.x = y) and
    ScopedAttributes (ie _property = copy("_property") to be easily shared between Processes
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)

        # Ensure that all attributes are named correctly
        # Iterate over all ScopedAttributes
        for t in type(self).mro():
            for name, value in t.__dict__.items():
                if isinstance(value, ScopedAttribute):
                    # Ensure name matches attribute name
                    compare_name = value.name
                    # Handle private attributes
                    if compare_name.startswith(PRIVATE_ATTRIBUTE_PREFIX):
                        compare_name = f"_{t.__name__}{value.name}"
                    if name != compare_name:
                        attribute_name = name
                        # Format private attributes
                        if PRIVATE_ATTRIBUTE_PREFIX in attribute_name:
                            attribute_name = PRIVATE_ATTRIBUTE_PREFIX + attribute_name.split(PRIVATE_ATTRIBUTE_PREFIX, 1)[1]
                        raise InvalidAttributeNameError(f"Name given for attribute '{attribute_name}' belonging to "
                                                        f"'{get_fully_qualified_name(self)}' is incorrect - was '{value.name}'")

    @abstractmethod
    def _handle_non_scoped_attribute_item(self, attributes: Dict[str, Any], key: str) -> None:
        """Handler for dealing with attributes that are not ScopedAttributes"""
        raise NotImplementedError()

    def __getstate__(self) -> ScopedAttributeState:

        # Detect super() implementation of __getstate__, using it if it exists
        super_ = super()
        attributes: Dict[str, Any] = {}
        if hasattr(super_, "__getstate__"):
            attributes[KEY_PARENT] = cast(ScopedAttributeStateHandler, super_).__getstate__()
        else:
            attributes = self.__dict__.copy()

        # Remove any scope_ids from ScopedAttribute key names so that values can be shared across scopes and handle any non-ScopedAttributes
        for key in list(attributes.keys()):
            if key.startswith(ATTRIBUTE_NAME_PREFIX):
                # Replace scope_id
                new_key = SCOPE_ID_REGEX.sub(SCOPE_ID_PLACEHOLDER, key)
                attributes[new_key] = attributes.pop(key)
            else:
                self._handle_non_scoped_attribute_item(attributes, key)

        # Find all ScopedAttributes in the object's class hierarchy
        for i in type(self).mro():
            for name, value in i.__dict__.items():
                if isinstance(value, ScopedAttribute):
                    value.assert_can_be_copied_to_another_process()

                    # Remove any "parent_only" or value_factory values, as these may contain knowingly unpickleable values
                    if value.accessible_scope == AccessibleScope.parent or attributes.get(value.get_attribute_id("value_factory"), False):
                        attributes.pop(value.get_attribute_id("value"), None)
                        attributes.pop(value.get_attribute_id("value_factory_called"), None)
                    if value.process_action == ProcessAction.SET_TO_NONE:
                        attributes[value.get_attribute_id("value")] = None

        return ScopedAttributeState(attributes)

    def __setstate__(self, state: ScopedAttributeState) -> None:

        # Re-add scope id to any ScopedAttribute variable names
        current_scope_id = get_current_scope_id()
        for key in list(state.attributes.keys()):
            if key.startswith(ATTRIBUTE_NAME_PREFIX):
                # Reinsert scope_id
                new_key = key.replace(SCOPE_ID_PLACEHOLDER, format_scope_id_for_attribute_id(current_scope_id))
                state.attributes[new_key] = state.attributes.pop(key)

        # Detect super() implementation of __setstate__, using it if it exists
        super_ = super()
        if hasattr(super_, "__setstate__"):
            cast(ScopedAttributeStateHandler, super_).__setstate__(state.attributes[KEY_PARENT])
        else:
            self.__dict__.update(state.attributes)


class ScopedAttributeStateHandler(ABC):

    @abstractmethod
    def __getstate__(self) -> ScopedAttributeState:
        raise NotImplementedError()

    @abstractmethod
    def __setstate__(self, state: ScopedAttributeState) -> None:
        raise NotImplementedError()


class InvalidAttributeNameError(AttributeError):
    pass
