from threading import Lock
from unittest import TestCase

from puma.attribute import child_only, copied, factory, parent_only, per_scope_value, scope_specific
from puma.attribute.attribute.attribute_accessed_from_invalid_scope_error import AttributeAccessedFromInvalidScopeError
from puma.attribute.mixin import ScopedAttributesCompatibilityMixin
from puma.environment import ProcessEnvironment
from puma.helpers.os import is_windows
from puma.helpers.testing.parameterized import parameterized
from puma.primitives import ThreadLock
from puma.runnable.decorator.run_in_child_scope import run_in_child_scope
from tests.attribute import get_access_child_from_parent_message, get_access_parent_from_child_message
from tests.attribute.scoped_attributes_test_helper import PropertiesDefinedAtClassLevel, PropertiesDefinedInInit, ScopedAttributesTestRunnable, \
    UnpickleablePropertiesDefinedAtClassLevel, UnpickleablePropertiesDefinedInInit
from tests.environment.parameterisation import EnvironmentTestParameters, environments


class ScopedAttributesSlowCompatibilityTest(TestCase):

    @parameterized(environments)
    def test_simple(self, env: EnvironmentTestParameters) -> None:
        environment = env.environment

        runnable = ScopedAttributesCompatibilityRunnable("parent", "child", "copied")
        with environment.create_runner(runnable) as runner:
            obj = runnable.object
            self.assertEqual("parent", obj.parent_only_prop)
            self.assertEqual("parent", obj.init_parent_only)

            with self.assertRaisesRegex(AttributeAccessedFromInvalidScopeError, get_access_child_from_parent_message("child_only_prop")):
                print(obj.child_only_prop)
            self.assertEqual("child", obj.init_child_only)

            self.assertEqual("copied", obj.copied_prop)
            self.assertEqual("copied", obj.init_copied)

            runner.start_blocking()

            runnable.verify_values()

            runner.stop()

    def test_unpickleable_values_inter_process(self) -> None:

        if not is_windows():
            self.skipTest("Skip this test on Linux as behaviour is inconsistent due to the use of non-ScopedAttributes")

        environment = ProcessEnvironment()

        runnable_with_unpickleable_init_properties = ScopedAttributesCompatibilityWithUnpickleablePropertiesDefinedInInitRunnable("parent", "child", "copied")
        with environment.create_runner(runnable_with_unpickleable_init_properties) as unpickleable_in_init_runner:
            obj1 = runnable_with_unpickleable_init_properties.object
            self.assertEqual("lock", obj1.init_unpickleable.__class__.__name__)

            with self.assertRaisesRegex(TypeError, "can't pickle _thread.lock objects"):
                unpickleable_in_init_runner.start_blocking()

        runnable_with_unpickleable_at_class_level = ScopedAttributesCompatibilityWithUnpickleablePropertiesDefinedAtClassLevelRunnable("parent", "child", "copied")
        with environment.create_runner(runnable_with_unpickleable_at_class_level) as unpickleable_at_class_level_runner:
            obj2 = runnable_with_unpickleable_at_class_level.object
            self.assertEqual("lock", obj2.unpickleable_prop.__class__.__name__)

            unpickleable_at_class_level_runner.start_blocking()

            runnable_with_unpickleable_at_class_level.verify_initial_state()

            unpickleable_at_class_level_runner.stop()

    @parameterized(environments)
    def test_multiple_instances_of_same_class(self, env: EnvironmentTestParameters) -> None:
        environment = env.environment
        runnable1 = SimpleCaseTestRunnable("test", "child-only-1", "child-only-2", "parent-only", "copied", ThreadLock())
        runnable2 = SimpleCaseTestRunnable("test", "1", "2", "p", "c", ThreadLock())

        with environment.create_runner(runnable1) as runner1, \
                environment.create_runner(runnable2) as runner2:

            runners = [runner1, runner2]

            # Ensure initial state is as expected
            self.assertEqual("parent-only", runnable1._parent_only)
            self.assertEqual("copied", runnable1._copied)
            self.assertEqual("lock", runnable1._unpickleable_parent_only.__class__.__name__)
            self.assertEqual("lock", runnable1._unpickleable_copied.__class__.__name__)
            with self.assertRaisesRegex(AttributeAccessedFromInvalidScopeError, get_access_child_from_parent_message("_child_only_1")):
                print(runnable1._child_only_1)
            with self.assertRaisesRegex(AttributeAccessedFromInvalidScopeError, get_access_child_from_parent_message("_child_only_2")):
                print(runnable1._child_only_2)
            with self.assertRaisesRegex(AttributeAccessedFromInvalidScopeError, get_access_child_from_parent_message("_unpickleable_child_only")):
                print(runnable1._unpickleable_child_only)

            for runner in runners:
                runner.start()
            for runner in runners:
                runner.wait_until_running()

            # noinspection SpellCheckingInspection
            runnable1.verify_initial_state("1-ylno-dlihc", "child-only-2", "copied")
            runnable2.verify_initial_state("1", "2", "c")

            for runner in runners:
                runner.stop()

            # Check end state


def reverse(string: str) -> str:
    return string[::-1]


class CompatClass(ScopedAttributesCompatibilityMixin, PropertiesDefinedInInit, PropertiesDefinedAtClassLevel):

    def __init__(self, parent_only_value: str, child_only_value: str, copied_value: str) -> None:
        PropertiesDefinedInInit.__init__(self, parent_only_value, child_only_value, copied_value)
        PropertiesDefinedAtClassLevel.__init__(self, parent_only_value, child_only_value, copied_value)


class ScopedAttributesCompatibilityRunnable(ScopedAttributesTestRunnable):
    object: CompatClass = copied("object")

    def __init__(self, parent_only_value: str, child_only_value: str, copied_value: str) -> None:
        super().__init__()
        self.object = CompatClass(parent_only_value, child_only_value, copied_value)

    @run_in_child_scope
    def verify_values(self) -> None:
        test_case = TestCase()
        obj = self.object

        with test_case.assertRaisesRegex(AttributeAccessedFromInvalidScopeError, get_access_parent_from_child_message("parent_only_prop")):
            print(obj.parent_only_prop)
        test_case.assertEqual("parent", obj.init_parent_only)

        test_case.assertEqual("child", obj.child_only_prop)
        test_case.assertEqual("child", obj.init_child_only)

        test_case.assertEqual("copied", obj.copied_prop)
        test_case.assertEqual("copied", obj.init_copied)


class CompatClassWithUnpickleablePropertiesDefinedInInit(ScopedAttributesCompatibilityMixin, UnpickleablePropertiesDefinedInInit, PropertiesDefinedAtClassLevel):

    def __init__(self, parent_only_value: str, child_only_value: str, copied_value: str) -> None:
        UnpickleablePropertiesDefinedInInit.__init__(self, parent_only_value, child_only_value, copied_value)
        PropertiesDefinedAtClassLevel.__init__(self, parent_only_value, child_only_value, copied_value)


class ScopedAttributesCompatibilityWithUnpickleablePropertiesDefinedInInitRunnable(ScopedAttributesTestRunnable):
    object: CompatClassWithUnpickleablePropertiesDefinedInInit = copied("object")

    def __init__(self, parent_only_value: str, child_only_value: str, copied_value: str) -> None:
        super().__init__()
        self.object = CompatClassWithUnpickleablePropertiesDefinedInInit(parent_only_value, child_only_value, copied_value)


class CompatClassWithUnpickleablePropertiesDefinedAtClassLevel(ScopedAttributesCompatibilityMixin, PropertiesDefinedInInit, UnpickleablePropertiesDefinedAtClassLevel):

    def __init__(self, parent_only_value: str, child_only_value: str, copied_value: str) -> None:
        PropertiesDefinedInInit.__init__(self, parent_only_value, child_only_value, copied_value)
        UnpickleablePropertiesDefinedAtClassLevel.__init__(self, parent_only_value, child_only_value, copied_value)


class ScopedAttributesCompatibilityWithUnpickleablePropertiesDefinedAtClassLevelRunnable(ScopedAttributesTestRunnable):
    object: CompatClassWithUnpickleablePropertiesDefinedAtClassLevel = copied("object")

    def __init__(self, parent_only_value: str, child_only_value: str, copied_value: str) -> None:
        super().__init__()
        self.object = CompatClassWithUnpickleablePropertiesDefinedAtClassLevel(parent_only_value, child_only_value, copied_value)

    @run_in_child_scope
    def verify_initial_state(self) -> None:
        test_case = TestCase()

        test_case.assertEqual("lock", self.object.unpickleable_prop.__class__.__name__)


class SharedContextProperties:
    _child_only_1: str = child_only("_child_only_1")
    _child_only_2: str = child_only("_child_only_2")
    _parent_only: str = parent_only("_parent_only")
    _copied: str = copied("_copied")
    _unpickleable_parent_only: Lock = parent_only("_unpickleable_parent_only")
    _unpickleable_child_only: Lock = child_only("_unpickleable_child_only")
    _unpickleable_copied: Lock = copied("_unpickleable_copied")


class Reverser:

    def __init__(self, original: str):
        self._original = original

    def reverse(self) -> str:
        return reverse(self._original)


class SimpleCaseTestRunnable(SharedContextProperties, ScopedAttributesTestRunnable):
    _safe: str = copied("_safe")

    def __init__(self, name: str, child_1: str, child_2: str, parent: str, copied_value: str, unpickleable_parent_only: Lock) -> None:
        super().__init__()
        self._safe = child_1
        self._child_only_1 = factory(Reverser(self._safe).reverse)
        self._child_only_2 = per_scope_value(child_2)
        self._parent_only = parent
        self._copied = copied_value
        self._unpickleable_parent_only = unpickleable_parent_only
        self._unpickleable_child_only = factory(ThreadLock)
        self._unpickleable_copied = scope_specific(unpickleable_parent_only, ThreadLock)

    @run_in_child_scope
    def verify_initial_state(self, expected_child_1_value: str, expected_child_2_value: str, expected_copied_value: str) -> None:
        test_case = TestCase()

        # Ensure initial state is as expected
        test_case.assertEqual(expected_child_1_value, self._child_only_1)
        test_case.assertEqual(expected_child_2_value, self._child_only_2)
        test_case.assertEqual(expected_copied_value, self._copied)
        test_case.assertEqual("lock", self._unpickleable_child_only.__class__.__name__)
        test_case.assertEqual("lock", self._unpickleable_copied.__class__.__name__)
        with test_case.assertRaisesRegex(AttributeAccessedFromInvalidScopeError, get_access_parent_from_child_message("_parent_only")):
            print(self._parent_only)
        with test_case.assertRaisesRegex(AttributeAccessedFromInvalidScopeError, get_access_parent_from_child_message("_unpickleable_parent_only")):
            print(self._unpickleable_parent_only)
