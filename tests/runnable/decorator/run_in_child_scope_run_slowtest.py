from time import monotonic
from unittest import TestCase

from puma.helpers.testing.parameterized import parameterized
from puma.runnable.remote_execution import CannotSetRemoteAttributeError
from puma.runnable.runner import ProcessRunner, ThreadRunner
from puma.scope_id import get_current_scope_id
from tests.runnable.decorator.run_in_child_scope_run_test_helpers import Parameters, RunnableTestClassChildStatusMessage, RunnableTestClassStatusMessage, \
    RunnableTestComplexClass, RunnableTestSubClass

envs = [
    Parameters("ThreadRunner", ThreadRunner),
    Parameters("ProcessRunner", ProcessRunner)
]


class RunInChildScopeRunTest(TestCase):

    @parameterized(envs)
    def test_ensure_decorated_and_undecorated_methods_work_as_expected_before_start(self, param: Parameters) -> None:
        with param:
            # Undecorated method works
            self.assertEqual(get_current_scope_id(), param.default_runnable.undecorated_method())

            # Decorated method raises an error
            with self.assertRaisesRegex(RuntimeError, "run_in_child_scope decorated methods may only be called once the Runnable has been started"):
                param.default_runnable.no_args()

    @parameterized(envs)
    def test_call_undecorated_method_runs_in_same_scope(self, param: Parameters) -> None:
        with param, param.create_runner() as runner:
            runnable = param.default_runnable
            runner.start_blocking()
            scope_id = runnable.undecorated_method()
            self.assertEqual(get_current_scope_id(), scope_id)

    @parameterized(envs)
    def test_call_methods_works_as_expected(self, param: Parameters) -> None:
        with param, param.create_runner() as runner:
            runnable = param.default_runnable
            runner.start_blocking()

            # Ensure each method wasn't called on the test thread
            no_args_result = runnable.no_args()
            no_args_call_details = runnable.wait_for_status_message(RunnableTestClassStatusMessage)
            self.assertEqual(102, no_args_result)
            self.assertEqual("no_args()", no_args_call_details.method)
            self.assertNotEqual(get_current_scope_id(), no_args_call_details.scope_id)

            some_args_result = runnable.some_args(123, "456")
            some_args_call_details = runnable.wait_for_status_message(RunnableTestClassStatusMessage)
            self.assertEqual(369, some_args_result)
            self.assertEqual("some_args(123, 456)", some_args_call_details.method)
            self.assertNotEqual(get_current_scope_id(), some_args_call_details.scope_id)

            # Ensure all methods were called on the same thread
            self.assertEqual(no_args_call_details.scope_id, some_args_call_details.scope_id)
            # Additional scope checks
            self.assertNotEqual(runnable.local_get_current_scope_id(), runnable.remote_get_current_scope_id())

    @parameterized(envs)
    def test_call_methods_works_when_returning_non_primitive_result(self, param: Parameters) -> None:
        runnable = RunnableTestComplexClass()
        with param, param.create_runner(runnable) as runner:
            runner.start_blocking()

            complex = runnable.get_complex_object(5, "hello")

            # Check simple values
            self.assertEqual(5, complex.get_a())
            self.assertEqual("hello", complex.get_b())

            subcomplex = complex.get_sub_complex()

            # Check that SubComplexObject method is being called in correct scope
            self.assertEqual(runnable.remote_get_current_scope_id(), subcomplex.get_scope_id())
            self.assertNotEqual(get_current_scope_id(), subcomplex.get_scope_id())

            # Check that non-primitive results can have their attributes accessed
            result_1 = subcomplex.returns_non_primitive(101, "hello", 202)
            self.assertEqual(101, result_1.primitive)
            self.assertEqual("hello", result_1.non_primitive.attr_1)
            self.assertEqual(202, result_1.non_primitive.attr_2)

            with self.assertRaisesRegex(CannotSetRemoteAttributeError, "Cannot set attribute 'attr_1' via RemoteObjectReference"):
                result_1.non_primitive.attr_1 = "new value"

            result_2 = subcomplex.returns_non_primitive(504, "bye", 302)
            self.assertEqual(504, result_2.primitive)
            self.assertEqual("bye", result_2.non_primitive.attr_1)
            self.assertEqual(302, result_2.non_primitive.attr_2)

            with self.assertRaisesRegex(CannotSetRemoteAttributeError, "Cannot set attribute 'attr_2' via RemoteObjectReference"):
                result_2.non_primitive.attr_2 = 105

    @parameterized(envs)
    def test_method_is_called_synchronously(self, param: Parameters) -> None:
        with param, param.create_runner() as runner:
            runnable = param.default_runnable
            runner.start_blocking()

            sleep_duration = 2

            # Ensure method called synchronously in a background scope
            start_time = monotonic()
            sync_method_result = runnable.sync_method(sleep_duration)
            duration = monotonic() - start_time
            self.assertGreaterEqual(duration, sleep_duration)
            self.assertEqual(sleep_duration, sync_method_result)

            sync_method_call_details = runnable.wait_for_status_message(RunnableTestClassStatusMessage)
            self.assertNotEqual(get_current_scope_id(), sync_method_call_details.scope_id)

    @parameterized(envs)
    def test_call_subclass_overridden_method_works_as_expected(self, param: Parameters) -> None:
        runnable = RunnableTestSubClass()
        with param.create_runner(runnable) as runner:
            runner.start_blocking()

            no_args_result = runnable.no_args()
            no_args_call_details = runnable.wait_for_status_message(RunnableTestClassStatusMessage)
            self.assertEqual(502, no_args_result)
            self.assertEqual("child:no_args()", no_args_call_details.method)
            self.assertNotEqual(get_current_scope_id(), no_args_call_details.scope_id)

    @parameterized(envs)
    def test_call_subclass_overridden_but_calls_super_method_works_as_expected(self, param: Parameters) -> None:
        runnable = RunnableTestSubClass()
        with param.create_runner(runnable) as runner:
            runner.start_blocking()

            some_args_result = runnable.some_args(456, "numbers")
            some_args_call_details_1 = runnable.wait_for_status_message(RunnableTestClassChildStatusMessage)
            some_args_call_details_2 = runnable.wait_for_status_message(RunnableTestClassStatusMessage)

            self.assertEqual(1455552, some_args_result)

            # Ensure overridden method published details
            self.assertEqual("child:some_args(456, numbers)", some_args_call_details_1.method)
            self.assertNotEqual(get_current_scope_id(), some_args_call_details_1.scope_id)

            # Ensure parent method published details
            self.assertEqual("some_args(456, numbers)", some_args_call_details_2.method)
            self.assertNotEqual(get_current_scope_id(), some_args_call_details_2.scope_id)

            self.assertEqual(some_args_call_details_1.scope_id, some_args_call_details_2.scope_id)
