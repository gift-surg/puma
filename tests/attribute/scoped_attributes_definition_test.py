from typing import Any

from puma.attribute import child_only, copied, factory, parent_only, per_scope_value, unmanaged
from puma.attribute.attribute.attribute_accessed_from_invalid_scope_error import AttributeAccessedFromInvalidScopeError
from puma.attribute.mixin import ScopedAttributesMixin
from puma.attribute.mixin.scoped_attributes_base_mixin import InvalidAttributeNameError
from puma.helpers.testing.logging.capture_logs import CaptureLogs
from puma.logging import LogLevel
from tests.attribute import get_access_child_from_parent_message
from tests.attribute.normal_and_compatibility.scoped_attributes_test_case import ScopedAttributesTestCase


class ScopedAttributesDefinitionTest(ScopedAttributesTestCase):
    parent_only_property: str = parent_only("parent_only_property")
    child_only_property: str = child_only("child_only_property")
    copied_property: str = copied("copied_property")
    unmanaged_property: str = unmanaged("unmanaged_property")

    def test_ensure_that_parent_only_property_can_be_set_directly(self) -> None:
        self.parent_only_property = "parent-value"
        self.assertEqual("parent-value", self.parent_only_property)

    def test_ensure_that_if_parent_only_property_given_factory_a_warning_is_logged(self) -> None:
        with CaptureLogs() as logs:
            self.parent_only_property = factory(factory_method)
            expected_message = "There is no need to use a ValueFactory for parent_only accessible property 'parent_only_property' - you may set the value directly"
            messages = logs.pop_captured_records().with_levels_in({LogLevel.warn}).containing_message(expected_message)
            self.assertTrue(messages)

    def test_ensure_that_child_only_property_can_be_set_with_a_factory(self) -> None:
        self.child_only_property = factory(factory_method)

    def test_ensure_that_child_only_property_can_be_set_with_a_per_context_value(self) -> None:
        self.child_only_property = per_scope_value("hello")

    def test_ensure_that_if_child_only_property_set_directly_an_error_is_raised(self) -> None:
        with self.assertRaisesRegex(AttributeAccessedFromInvalidScopeError, get_access_child_from_parent_message("child_only_property")):
            self.child_only_property = "child-value"

    def test_ensure_that_copied_property_can_be_set_directly(self) -> None:
        self.copied_property = "copy-value"
        self.assertEqual("copy-value", self.copied_property)

    def test_ensure_that_copied_property_can_be_set_with_a_factory(self) -> None:
        self.copied_property = factory(factory_method)
        self.assertEqual("factory-string", self.copied_property)

    def test_ensure_that_copied_property_can_be_set_with_a_per_context_value(self) -> None:
        self.copied_property = per_scope_value("hello")
        self.assertEqual("hello", self.copied_property)

    def test_ensure_that_unmanaged_property_can_be_set_and_read(self) -> None:
        self.unmanaged_property = "hello"
        self.assertEqual("hello", self.unmanaged_property)

    def test_ensure_that_errors_are_raised_within_init_when_attributes_are_first_set(self) -> None:
        # Ensure ValidSubClass can be created successfully
        ValidSubClass()

        with self.assertRaisesInvalidAttributeTypeError("_unmanaged_attr"):
            InvalidSubClass()

    def test_factory_given_unpickleable_method(self) -> None:
        # Ensure valid factories work
        factory(list)
        factory(dict)
        factory(factory_method)

        with self.assertRaisesRegex(TypeError, "ValueFactory must be pickleable"):
            factory(lambda: "lambda-value")

        with self.assertRaisesRegex(TypeError, "ValueFactory must be pickleable"):
            class PrivateClass:

                def method(self) -> str:
                    return "private-class-method"

            factory(PrivateClass().method)

    def test_ensure_mismatched_names_raise_an_error(self) -> None:
        def assert_raises_invalid_attribute_name_error(class_name: str, expected_name: str, actual_name: str) -> Any:
            expected_error_message = f"Name given for attribute '{expected_name}' belonging to '{class_name}' is incorrect - was '{actual_name}'"

            return self.assertRaisesRegex(InvalidAttributeNameError, expected_error_message)

        with assert_raises_invalid_attribute_name_error(
                "tests.attribute.scoped_attributes_definition_test.ClassWithMismatchedNames",
                "_attr2",
                "_attr_two"):
            ClassWithMismatchedNames()

        with assert_raises_invalid_attribute_name_error(
                "tests.attribute.scoped_attributes_definition_test.ClassWithMismatchedPrivateAttributeNames",
                "__private_2",
                "__private_two"):
            ClassWithMismatchedPrivateAttributeNames()


def factory_method() -> str:
    return "factory-string"


class SimpleScopedAttributesClass(ScopedAttributesMixin):
    _managed_attr: int = parent_only("_managed_attr")

    def __init__(self) -> None:
        super().__init__()
        self._managed_attr = 15


# Use subclasses to double check that this mechanism works through the class hierarchy
class ValidSubClass(SimpleScopedAttributesClass):

    def __init__(self) -> None:
        super().__init__()
        self._managed_attr = 25


class InvalidSubClass(SimpleScopedAttributesClass):

    def __init__(self) -> None:
        super().__init__()
        self._unmanaged_attr = 16


class ClassWithMismatchedNames(ScopedAttributesMixin):
    _attr: str = copied("_attr")
    _attr2: str = copied("_attr_two")


class ClassWithMismatchedPrivateAttributeNames(ScopedAttributesMixin):
    __private: str = copied("__private")
    __private_2: str = copied("__private_two")
