from typing import Type

from puma.attribute.mixin import ScopedAttributesCompatibilityMixin, ScopedAttributesMixin
from tests.attribute.normal_and_compatibility.properties_defined_in_init_and_at_class_level import ScopedAttributesCompatPropertiesDefinedInInitAndAtClassLevelTestCase
from tests.attribute.normal_and_compatibility.scoped_attributes_test_case import ScopedAttributesTestCase
from tests.attribute.normal_and_compatibility.verification_interface import VerificationInterface
from tests.attribute.scoped_attributes_test_helper import PropertiesDefinedInInitAndUnpickleablePropertiesDefinedAtClassLevel


class ScopedAttributesPropertiesDefinedInInitAndUnpickleablePropertiesDefinedAtClassLevel(PropertiesDefinedInInitAndUnpickleablePropertiesDefinedAtClassLevel,
                                                                                          ScopedAttributesMixin):
    pass


class ScopedAttributesCompatPropertiesDefinedInInitAndUnpickleablePropertiesDefinedAtClassLevel(PropertiesDefinedInInitAndUnpickleablePropertiesDefinedAtClassLevel,
                                                                                                ScopedAttributesCompatibilityMixin):
    pass


class ScopedAttributesCompatPropertiesDefinedInInitAndUnpickleablePropertiesDefinedAtClassLevelTestCase \
            (VerificationInterface[PropertiesDefinedInInitAndUnpickleablePropertiesDefinedAtClassLevel]):

    def __init__(self) -> None:
        super().__init__()
        self._parent_test = ScopedAttributesCompatPropertiesDefinedInInitAndAtClassLevelTestCase()
        self.unpickle_obj_id = 0

    def get_test_class(self) -> Type[PropertiesDefinedInInitAndUnpickleablePropertiesDefinedAtClassLevel]:
        return ScopedAttributesCompatPropertiesDefinedInInitAndUnpickleablePropertiesDefinedAtClassLevel

    def local_verification_method(self, test_case: ScopedAttributesTestCase, obj: PropertiesDefinedInInitAndUnpickleablePropertiesDefinedAtClassLevel) -> None:
        self._parent_test.local_verification_method(test_case, obj)
        test_case.assertEqual("lock", obj.unpickleable_prop.__class__.__name__)
        self.unpickle_obj_id = id(obj.unpickleable_prop)

    def remote_verification_method(self, test_case: ScopedAttributesTestCase, obj: PropertiesDefinedInInitAndUnpickleablePropertiesDefinedAtClassLevel) -> None:
        self._parent_test.remote_verification_method(test_case, obj)
        test_case.assertEqual("lock", obj.unpickleable_prop.__class__.__name__)
        # Ensure that Compat mode with class level attribute points to a different object
        test_case.assertNotEqual(self.unpickle_obj_id, id(obj.unpickleable_prop))
