from typing import Type

from puma.attribute.attribute.attribute_accessed_from_invalid_scope_error import AttributeAccessedFromInvalidScopeError
from puma.attribute.mixin import ScopedAttributesCompatibilityMixin, ScopedAttributesMixin
from tests.attribute import get_access_child_from_parent_message, get_access_parent_from_child_message
from tests.attribute.normal_and_compatibility.scoped_attributes_test_case import ScopedAttributesTestCase
from tests.attribute.normal_and_compatibility.verification_interface import VerificationInterface
from tests.attribute.scoped_attributes_test_helper import PropertiesDefinedInInitAndAtClassLevel


class ScopedAttributesPropertiesDefinedInInitAndAtClassLevel(PropertiesDefinedInInitAndAtClassLevel, ScopedAttributesMixin):
    pass


class ScopedAttributesCompatPropertiesDefinedInInitAndAtClassLevel(PropertiesDefinedInInitAndAtClassLevel, ScopedAttributesCompatibilityMixin):
    pass


class ScopedAttributesCompatPropertiesDefinedInInitAndAtClassLevelTestCase(VerificationInterface[PropertiesDefinedInInitAndAtClassLevel]):

    def get_test_class(self) -> Type[PropertiesDefinedInInitAndAtClassLevel]:
        return ScopedAttributesCompatPropertiesDefinedInInitAndAtClassLevel

    def local_verification_method(self, test_case: ScopedAttributesTestCase, obj: PropertiesDefinedInInitAndAtClassLevel) -> None:
        test_case.assertEqual("parent", obj.parent_only_prop)
        test_case.assertEqual("parent", obj.init_parent_only)

        with test_case.assertRaisesRegex(AttributeAccessedFromInvalidScopeError, get_access_child_from_parent_message("child_only_prop")):
            print(obj.child_only_prop)
        test_case.assertEqual("child", obj.init_child_only)

        test_case.assertEqual("copied", obj.copied_prop)
        test_case.assertEqual("copied", obj.init_copied)

        test_case.assertEqual("unmanaged-value", obj.unmanaged_prop)

    def remote_verification_method(self, test_case: ScopedAttributesTestCase, obj: PropertiesDefinedInInitAndAtClassLevel) -> None:
        with test_case.assertRaisesRegex(AttributeAccessedFromInvalidScopeError, get_access_parent_from_child_message("parent_only_prop")):
            print(obj.parent_only_prop)
        test_case.assertEqual("parent", obj.init_parent_only)

        test_case.assertEqual("child", obj.child_only_prop)
        test_case.assertEqual("child", obj.init_child_only)

        test_case.assertEqual("copied", obj.copied_prop)
        test_case.assertEqual("copied", obj.init_copied)

        test_case.assertEqual("unmanaged-value", obj.unmanaged_prop)
