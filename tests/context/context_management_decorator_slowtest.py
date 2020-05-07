from unittest import TestCase

from puma.attribute import copied
from puma.context import ContextManager, MustBeContextManagedError, ensure_used_within_context_manager, must_be_context_managed
from puma.helpers.testing.mixin import NotATestCase
from puma.runnable import CommandDrivenRunnable
from puma.runnable.decorator.run_in_child_scope import run_in_child_scope
from puma.runnable.runner import ProcessRunner


class ContextManagementDecoratorSlowTest(TestCase):

    def test_decorated_classes_can_be_used_on_runnables(self) -> None:
        context_managed_object = ContextManagedObject()
        runnable = TestRunnable("Test Runnable", context_managed_object)
        with ProcessRunner(runnable) as runner:
            runner.start_blocking()
            runnable.call_obj_methods()


@must_be_context_managed
class ContextManagedObject(ContextManager["ContextManagedObject"]):

    def __enter__(self) -> "ContextManagedObject":
        return self

    def unchecked_method(self) -> str:
        return "unchecked"

    @ensure_used_within_context_manager
    def checked_method(self) -> str:
        return "checked"


class TestRunnable(CommandDrivenRunnable, NotATestCase):
    _obj: ContextManagedObject = copied("_obj")

    def __init__(self, name: str, obj: ContextManagedObject) -> None:
        super().__init__(name, [])
        self._obj = obj

    @run_in_child_scope
    def call_obj_methods(self) -> None:
        test_case = TestCase()

        test_case.assertEqual("unchecked", self._obj.unchecked_method())

        with test_case.assertRaisesRegex(MustBeContextManagedError, "Must be context managed"):
            self._obj.checked_method()

        with self._obj:
            test_case.assertEqual("checked", self._obj.checked_method())
