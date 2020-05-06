from calendar import IllegalMonthError
from unittest import TestCase

from puma.attribute import ProcessAction, ThreadAction, copied, factory, manually_managed, per_scope_value
from puma.attribute.attribute.sharing_attribute_between_scopes_not_allowed_error import SharingAttributeBetweenScopesNotAllowedError
from puma.attribute.mixin import ScopedAttributesCompatibilityMixin, ScopedAttributesMixin
from puma.environment import Environment, ProcessEnvironment, ThreadEnvironment
from puma.helpers.testing.parameterized import parameterized
from puma.runnable import Runnable
from puma.runnable.decorator.run_in_child_scope import run_in_child_scope
from puma.scope_id import get_current_scope_id
from tests.attribute.scoped_attributes_test_helper import ScopedAttributesTestRunnable
from tests.environment.parameterisation import EnvironmentTestParameters, environments


class ScopedAttributesThreadsSlowTest(TestCase):

    @parameterized(environments)
    def test_ensure_values_cannot_be_edited_across_context_boundaries(self, env: EnvironmentTestParameters) -> None:
        self._call_test_case(env.environment, SimpleCaseTestRunnable())

    def test_ensure_shared_between_thread_values_work_between_threads(self) -> None:
        self._call_test_case(ThreadEnvironment(), HasSharedValuesTestRunnable())

    def test_ensure_shared_between_thread_values_work_between_threads_repeated_100_times(self) -> None:
        fail_count = 0
        error_count = 0
        for i in range(100):
            try:
                print("Running #", i + 1)
                self.test_ensure_shared_between_thread_values_work_between_threads()
            except AssertionError as ae:
                print("Failed", str(ae))
                fail_count += 1
            except IllegalMonthError as e:
                print("Errored", str(e))
                error_count += 1

        if fail_count > 0 or error_count > 0:
            self.fail(f"Failures: {fail_count}; Errors: {error_count}")

    def test_ensure_shared_between_thread_values_raise_an_error_if_used_with_processes(self) -> None:
        with self.assertRaisesRegex(SharingAttributeBetweenScopesNotAllowedError,
                                    "Attribute 'shared' may not be passed between Processes as its ProcessAction is 'NOT_ALLOWED'"):
            self._call_test_case(ProcessEnvironment(), HasSharedValuesTestRunnable())

    def _call_test_case(self, environment: Environment, runnable: "SimpleCaseTestRunnable") -> None:
        with environment.create_runner(runnable) as runner:
            # Ensure initial state is as expected
            self.assertEqual("initial-value", runnable.copied_atr)
            self.assertEqual("initial-object-value", runnable.copied_object.child.value)
            self.assertEqual("initial-managed-object-value", runnable.copied_complex.child.child.child)
            self.assertEqual("per-context-value", runnable.per_context_value)
            self.assertEqual(f"created-value-{get_current_scope_id()}", runnable.factory)

            runnable.copied_atr = "pre-start-copied"
            runnable.copied_object.child.value = "pre-start-object-value"
            runnable.copied_complex.child.child.child = "pre-start-managed-object-value"
            runnable.per_context_value = "pre-start-per_context_value"
            runnable.factory = "pre-start-factory"
            self.assertEqual("pre-start-copied", runnable.copied_atr)
            self.assertEqual("pre-start-object-value", runnable.copied_object.child.value)
            self.assertEqual("pre-start-managed-object-value", runnable.copied_complex.child.child.child)
            self.assertEqual("pre-start-per_context_value", runnable.per_context_value)
            self.assertEqual("pre-start-factory", runnable.factory)

            runner.start_blocking()

            runnable.copied_atr = "local-value"
            self.assertEqual("local-value", runnable.copied_atr)

            runnable.copied_object.child.value = "local-object-value"
            self.assertEqual("local-object-value", runnable.copied_object.child.value)

            runnable.copied_complex.child.child.child = "local-managed-object-value"
            self.assertEqual("local-managed-object-value", runnable.copied_complex.child.child.child)

            runnable.verify_remote_values_after_start()

            # TODO: Improve this test / remove this hack
            if isinstance(runnable, HasSharedValuesTestRunnable):
                runnable.shared = "local-shared-value"
                self.assertEqual("local-shared-value", runnable.shared)

            # Ensure value hasn't changed
            runnable.verify_remote_values_after_local_update()

            # Update remote values
            runnable.update_remote_property_value("remote-value")

            # Ensure local values haven't changed
            self.assertEqual("local-value", runnable.copied_atr)
            self.assertEqual("local-object-value", runnable.copied_object.child.value)
            self.assertEqual("local-managed-object-value", runnable.copied_complex.child.child.child)

            if isinstance(runnable, HasSharedValuesTestRunnable):
                self.assertEqual("remote-value", runnable.shared)

            runner.stop()


def create_value() -> str:
    return f"created-value-{get_current_scope_id()}"


class SomeObject1:
    def __init__(self, child_object: "SomeObject2") -> None:
        self.child = child_object


class SomeObject2(ScopedAttributesCompatibilityMixin):
    def __init__(self, value: str) -> None:
        super().__init__()
        self.value = value


class ManagedObject1(ScopedAttributesMixin):
    child: "ManagedObject2" = copied("child")

    def __init__(self, child: "ManagedObject2") -> None:
        super().__init__()
        self.child = child


class ManagedObject2(ScopedAttributesMixin):
    child: "ManagedObject3" = copied("child")

    def __init__(self, child: "ManagedObject3") -> None:
        super().__init__()
        self.child = child


class ManagedObject3(ScopedAttributesMixin):
    child: str = copied("child")
    runnable: Runnable = copied("runnable")  # Include reference back to parent Runnable in order to detect infinite recursion

    def __init__(self, child: str, runnable: Runnable) -> None:
        super().__init__()
        self.child = child
        self.runnable = runnable


class SimpleCaseTestRunnable(ScopedAttributesTestRunnable):
    copied_atr: str = copied("copied_atr")
    copied_object: SomeObject1 = copied("copied_object")
    copied_complex: ManagedObject1 = copied("copied_complex")
    per_context_value: str = copied("per_context_value")
    factory: str = copied("factory")

    def __init__(self) -> None:
        super().__init__()
        self.copied_atr = "initial-value"
        self.copied_object = SomeObject1(SomeObject2("initial-object-value"))
        self.copied_complex = ManagedObject1(ManagedObject2(ManagedObject3("initial-managed-object-value", self)))
        self.per_context_value = per_scope_value("per-context-value")
        self.factory = factory(create_value)

    @run_in_child_scope
    def verify_remote_values_after_start(self) -> None:
        test_case = TestCase()
        test_case.assertEqual("pre-start-copied", self.copied_atr, "verify_values_after_start")
        test_case.assertEqual("pre-start-object-value", self.copied_object.child.value, "verify_values_after_start")
        test_case.assertEqual("pre-start-managed-object-value", self.copied_complex.child.child.child, "verify_values_after_start")
        test_case.assertEqual("per-context-value", self.per_context_value, "verify_values_after_start")
        test_case.assertEqual(f"created-value-{get_current_scope_id()}", self.factory, "verify_values_after_start")

    def verify_remote_values_after_local_update(self) -> None:
        self.verify_remote_values_after_start()

    @run_in_child_scope
    def update_remote_property_value(self, new_value: str) -> None:
        test_case = TestCase()
        self.copied_atr = new_value
        test_case.assertEqual(new_value, self.copied_atr)

        self.copied_object.child.value = new_value
        test_case.assertEqual(new_value, self.copied_object.child.value)

        self.copied_complex.child.child.child = new_value
        test_case.assertEqual(new_value, self.copied_complex.child.child.child)


class HasSharedValuesTestRunnable(SimpleCaseTestRunnable):
    shared: str = manually_managed("shared", ThreadAction.SHARED, ProcessAction.NOT_ALLOWED)

    def __init__(self) -> None:
        super().__init__()
        self.shared = "shared-value"

    @run_in_child_scope
    def update_remote_property_value(self, new_value: str) -> None:
        super().update_remote_property_value(new_value)
        self.shared = new_value
