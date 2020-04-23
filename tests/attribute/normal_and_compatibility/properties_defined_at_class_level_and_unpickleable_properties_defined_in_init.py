from typing import Optional, Type

from puma.attribute.mixin import ScopedAttributesCompatibilityMixin, ScopedAttributesMixin
from tests.attribute.normal_and_compatibility.properties_defined_in_init_and_at_class_level import ScopedAttributesCompatPropertiesDefinedInInitAndAtClassLevelTestCase
from tests.attribute.normal_and_compatibility.scoped_attributes_test_case import ScopedAttributesTestCase
from tests.attribute.normal_and_compatibility.verification_interface import VerificationInterface
from tests.attribute.scoped_attributes_test_helper import PropertiesDefinedAtClassLevelAndUnpickleablePropertiesDefinedInInit


class ScopedAttributesPropertiesDefinedAtClassLevelAndUnpickleablePropertiesDefinedInInit(PropertiesDefinedAtClassLevelAndUnpickleablePropertiesDefinedInInit,
                                                                                          ScopedAttributesMixin):
    pass


class ScopedAttributesCompatPropertiesDefinedAtClassLevelAndUnpickleablePropertiesDefinedInInit(PropertiesDefinedAtClassLevelAndUnpickleablePropertiesDefinedInInit,
                                                                                                ScopedAttributesCompatibilityMixin):
    pass


class ScopedAttributesCompatPropertiesDefinedAtClassLevelAndUnpickleablePropertiesDefinedInInitTestCase \
            (VerificationInterface[PropertiesDefinedAtClassLevelAndUnpickleablePropertiesDefinedInInit]):

    def __init__(self) -> None:
        super().__init__()
        self._parent_test = ScopedAttributesCompatPropertiesDefinedInInitAndAtClassLevelTestCase()
        self.unpickle_obj_id = 0

    def get_test_class(self) -> Type[PropertiesDefinedAtClassLevelAndUnpickleablePropertiesDefinedInInit]:
        return ScopedAttributesCompatPropertiesDefinedAtClassLevelAndUnpickleablePropertiesDefinedInInit

    def local_verification_method(self, test_case: ScopedAttributesTestCase, obj: PropertiesDefinedAtClassLevelAndUnpickleablePropertiesDefinedInInit) -> None:
        self._parent_test.local_verification_method(test_case, obj)
        test_case.assertEqual("lock", obj.init_unpickleable.__class__.__name__)
        self.unpickle_obj_id = id(obj.init_unpickleable)

    def get_process_start_expected_error_message(self) -> Optional[str]:
        return "can't pickle _thread.lock objects"

    def remote_verification_method(self, test_case: ScopedAttributesTestCase, obj: PropertiesDefinedAtClassLevelAndUnpickleablePropertiesDefinedInInit) -> None:
        self._parent_test.remote_verification_method(test_case, obj)
        test_case.assertEqual("lock", obj.init_unpickleable.__class__.__name__)
        # ThreadRunner only - will fail before it gets here with ProcessRunner
        # Ensure that Compat mode with init-defined attribute points to the same object
        test_case.assertEqual(self.unpickle_obj_id, id(obj.init_unpickleable))
