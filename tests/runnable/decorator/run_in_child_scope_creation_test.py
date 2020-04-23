from abc import ABC, abstractmethod
from unittest import TestCase

from puma.runnable import CommandDrivenRunnable
from puma.runnable.decorator.run_in_child_scope import run_in_child_scope
from puma.runnable.runner import ThreadRunner


class RunInChildScopeCreationTest(TestCase):

    def test_error_raised_if_used_on_non_runnable(self) -> None:
        with self.assertRaisesRegex(TypeError, "run_in_child_scope decorator may only be used on methods within a Runnable"):
            NonRunnableTestClass().some_method()

    def test_ensure_decorated_methods_have_expected_name_and_docstring(self) -> None:
        test_runnable = RunnableTestClass()
        self.assertEqual("method_that_returns_none", test_runnable.method_that_returns_none.__name__)
        self.assertEqual("This method returns None", test_runnable.method_that_returns_none.__doc__)
        self.assertEqual("method_with_no_return_type", test_runnable.method_with_no_return_type.__name__)
        self.assertEqual(None, test_runnable.method_with_no_return_type.__doc__)

    def test_ensure_error_is_raised_if_decorated_method_called_before_runnable_is_started(self) -> None:
        test_runnable = RunnableTestClass()
        with self.assertRaisesRegex(RuntimeError, "run_in_child_scope decorated methods may only be called once the Runnable has been started"):
            test_runnable.method_that_takes_params()  # type: ignore

    def test_ensure_error_is_raised_if_method_called_with_incorrect_parameters(self) -> None:
        test_runnable = RunnableTestClass()
        with ThreadRunner(test_runnable) as runner:
            runner.start_blocking()

            with self.assertRaisesRegex(TypeError, "missing a required argument: 'first'"):
                test_runnable.method_that_takes_params()  # type: ignore

            with self.assertRaisesRegex(TypeError, "missing a required argument: 'second'"):
                test_runnable.method_that_takes_params("first")  # type: ignore

            # Ensure it doesn't raise an error when called correctly
            test_runnable.method_that_takes_params("first", 45)


class NonRunnableTestClass:

    @run_in_child_scope
    def some_method(self) -> None:
        print("Hello, world")


class RunnableTestClass(CommandDrivenRunnable):

    def __init__(self) -> None:
        super().__init__("RunInChildScope Test Runnable", [])

    @run_in_child_scope
    def method_that_returns_string(self) -> str:
        return "hello"

    @run_in_child_scope
    def method_with_no_return_type(self):  # type: ignore
        pass

    @run_in_child_scope
    def method_that_returns_none(self) -> None:
        """This method returns None"""
        pass

    @run_in_child_scope
    def method_that_takes_params(self, first: str, second: int) -> None:
        pass


# These two classes are not used in test code but are used to verify that the Typing of run_in_child_scope is working as expected
#   Namely, that an overridden method of a base class retains the same signature
class AbstractBaseClass(ABC):

    @abstractmethod
    def do_something(self, a: str, b: int) -> None:
        raise NotImplementedError()


class ImplementationClass(AbstractBaseClass):

    @run_in_child_scope
    def do_something(self, a: str, b: int) -> None:
        pass
