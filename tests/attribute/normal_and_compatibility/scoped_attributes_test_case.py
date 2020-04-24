from typing import Any
from unittest import TestCase

from puma.attribute.mixin.scoped_attributes_mixin import InvalidAttributeTypeError


class ScopedAttributesTestCase(TestCase):

    def assertRaisesInvalidAttributeTypeError(self, attribute_name: str) -> Any:
        msg = f"Using non-ScopedAttributes with ScopedAttributesMixin is not allowed. " \
              f"Please define the variable '{attribute_name}' at the class level using any of parent_only, child_only or copied"
        return self.assertRaisesRegex(InvalidAttributeTypeError, msg)
