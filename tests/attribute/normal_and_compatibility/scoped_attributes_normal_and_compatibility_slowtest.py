from contextlib import ExitStack
from typing import Any, Callable, Generic, TypeVar

from puma.attribute import copied
from puma.environment import ProcessEnvironment
from puma.helpers.os import is_windows
from puma.runnable.decorator.run_in_child_scope import run_in_child_scope
from tests.attribute.normal_and_compatibility.properties_defined_at_class_level_and_unpickleable_properties_defined_in_init import \
    ScopedAttributesCompatPropertiesDefinedAtClassLevelAndUnpickleablePropertiesDefinedInInitTestCase, \
    ScopedAttributesPropertiesDefinedAtClassLevelAndUnpickleablePropertiesDefinedInInit
from tests.attribute.normal_and_compatibility.properties_defined_in_init_and_at_class_level import ScopedAttributesCompatPropertiesDefinedInInitAndAtClassLevelTestCase, \
    ScopedAttributesPropertiesDefinedInInitAndAtClassLevel
from tests.attribute.normal_and_compatibility.properties_defined_in_init_and_unpickleable_properties_defined_at_class_level import \
    ScopedAttributesCompatPropertiesDefinedInInitAndUnpickleablePropertiesDefinedAtClassLevelTestCase, \
    ScopedAttributesPropertiesDefinedInInitAndUnpickleablePropertiesDefinedAtClassLevel
from tests.attribute.normal_and_compatibility.scoped_attributes_test_case import ScopedAttributesTestCase
from tests.attribute.normal_and_compatibility.verification_interface import VerificationInterface
from tests.attribute.scoped_attributes_test_helper import ScopedAttributesTestRunnable
from tests.environment.parameterisation import EnvironmentTestParameters, environments
from tests.parameterized import parameterized

T = TypeVar("T")


class ScopedAttributesSlowNormalAndCompatibilityTest(ScopedAttributesTestCase):

    @parameterized(environments)
    def test_properties_defined_in_init_and_at_class_level(self, env: EnvironmentTestParameters) -> None:
        self.ensure_instance_creation_raises_error(ScopedAttributesPropertiesDefinedInInitAndAtClassLevel)

    @parameterized(environments)
    def test_compat_properties_defined_in_init_and_at_class_level(self, env: EnvironmentTestParameters) -> None:
        self.run_test_case(env, ScopedAttributesCompatPropertiesDefinedInInitAndAtClassLevelTestCase())

    @parameterized(environments)
    def test_properties_defined_in_init_and_unpickleable_properties_defined_at_class_level(self, env: EnvironmentTestParameters) -> None:
        self.ensure_instance_creation_raises_error(ScopedAttributesPropertiesDefinedInInitAndUnpickleablePropertiesDefinedAtClassLevel)

    @parameterized(environments)
    def test_compat_properties_defined_in_init_and_unpickleable_properties_defined_at_class_level(self, env: EnvironmentTestParameters) -> None:
        self.run_test_case(env, ScopedAttributesCompatPropertiesDefinedInInitAndUnpickleablePropertiesDefinedAtClassLevelTestCase())

    @parameterized(environments)
    def test_properties_defined_at_class_level_and_unpickleable_properties_defined_in_init(self, env: EnvironmentTestParameters) -> None:
        self.ensure_instance_creation_raises_error(ScopedAttributesPropertiesDefinedAtClassLevelAndUnpickleablePropertiesDefinedInInit)

    @parameterized(environments)
    def test_compat_properties_defined_at_class_level_and_unpickleable_properties_defined_in_init(self, env: EnvironmentTestParameters) -> None:

        if not is_windows() and isinstance(env.environment, ProcessEnvironment):
            self.skipTest("Skip this test on Linux as behaviour is inconsistent due to the use of non-ScopedAttributes")

        self.run_test_case(env, ScopedAttributesCompatPropertiesDefinedAtClassLevelAndUnpickleablePropertiesDefinedInInitTestCase())

    def ensure_instance_creation_raises_error(self, class_: Callable[[str, str, str], Any]) -> None:
        with self.assertRaisesInvalidAttributeTypeError("init_parent_only"):
            class_("parent", "child", "copied")

    def run_test_case(self, env: EnvironmentTestParameters, test_case: "VerificationInterface") -> None:
        environment = env.environment

        obj = test_case.get_test_class()("parent", "child", "copied")

        runnable = ScopedAttributesSlowNormalAndCompatibilityTestRunnable(obj, test_case)
        with environment.create_runner(runnable) as runner:
            test_case.local_verification_method(self, runnable.object)

            runner_context = ExitStack()

            if isinstance(environment, ProcessEnvironment):
                process_start_expected_error_msg = test_case.get_process_start_expected_error_message()
                if process_start_expected_error_msg:
                    runner_context.enter_context(self.assertRaisesRegex(TypeError, process_start_expected_error_msg))

            with runner_context:
                runner.start_blocking()
                runnable.verify_values()
                runner.stop()


T2 = TypeVar("T2")


class ScopedAttributesSlowNormalAndCompatibilityTestRunnable(Generic[T2], ScopedAttributesTestRunnable):
    object: T2 = copied("object")
    test_case: VerificationInterface[T2] = copied("test_case")

    def __init__(self, obj: T2, test_case: VerificationInterface[T2]) -> None:
        super().__init__()
        self.object = obj
        self.test_case = test_case

    @run_in_child_scope
    def verify_values(self) -> None:
        self.test_case.remote_verification_method(ScopedAttributesSlowNormalAndCompatibilityTest(), self.object)
