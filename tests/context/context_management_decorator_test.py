from typing import Any, Generic, TypeVar
from unittest import TestCase

from puma.context import ContextManager, Exit_1, Exit_2, Exit_3, MustBeContextManagedError, ensure_used_within_context_manager, must_be_context_managed


class ContextManagementDecoratorTest(TestCase):

    def test_ensure_an_error_is_raised_if_non_context_manager_class_is_decorated(self) -> None:
        with self.assertRaisesRegex(RuntimeError,
                                    "must_be_context_managed decorator may only be used on a subclass of typing.ContextManger, ideally puma.context.ContextManager"):
            # Ignore type errors as this class is intentionally not a ContextManager
            must_be_context_managed(NonContextManagedClass)  # type: ignore

    def test_ensure_supports_generic_classes(self) -> None:
        # Create subclasses of decorated generic classes
        class SubClass1(GenericClass1[Any]):
            pass

        class SubClass3(GenericClass3[Any, Any, Any]):
            pass

    def test_ensure_init_variables_are_correctly_propagated(self) -> None:
        m = MustBeManaged("abc", 123)
        self.assertEqual("abc", m._string_var)
        self.assertEqual(123, m._num_var)

    def test_ensure_that_enter_and_exit_methods_are_actually_called(self) -> None:
        m = MustBeManaged("abc", 123)
        self.assertFalse(m._enter_method_was_called)
        self.assertFalse(m._exit_method_was_called)

        with m:
            self.assertTrue(m._enter_method_was_called)
            self.assertFalse(m._exit_method_was_called)

        self.assertTrue(m._exit_method_was_called)

    def test_ensure_errors_raised_when_expected(self) -> None:
        m = MustBeManaged("abc", 123)

        # No errors here
        m.call_method_outside_context(45, "hello")

        with self.assertRaisesRegex(RuntimeError, "Must be context managed"):
            m.call_method_inside_context(2, "bye")

        with m:
            m.call_method_inside_context(2, "bye")

        with self.assertRaisesRegex(RuntimeError, "Must be context managed"):
            m.call_method_inside_context(2, "bye")

    def test_ensure_errors_raised_when_expected_when_subclassed(self) -> None:
        s = SubClass("abc", 123)

        # No errors here
        s.call_method_outside_context(45, "hello")
        s.subclass_method()

        with self.assertRaisesRegex(RuntimeError, "Must be context managed"):
            s.call_method_inside_context(2, "bye")

        with s:
            s.call_method_inside_context(2, "bye")

        with self.assertRaisesRegex(RuntimeError, "Must be context managed"):
            s.call_method_inside_context(2, "bye")

    def test_ensure_error_raised_if_method_decorated_inside_non_decorated_class(self) -> None:
        undecorated = UndecoratedClass()

        with self.assertRaisesRegex(RuntimeError, "Unable to determine if instance has been context managed - is the class annotated with @must_be_context_managed?"):
            undecorated.call_a_method()

    def test_ensure_decorated_methods_can_be_safely_called_from_within_exit_method(self) -> None:
        obj = CallsDecoratedMethodFromExit()

        with self.assertRaisesRegex(MustBeContextManagedError, "Must be context managed"):
            obj.cleanup()

        with obj:
            pass  # Do nothing, but ensure no errors are raised

    def test_ensure_context_state_still_tracked_even_if_exit_raises_an_error(self) -> None:
        obj = RaisesAnErrorDuringExit()

        with self.assertRaisesRegex(MustBeContextManagedError, "Must be context managed"):
            obj.call_inside_context()

        with self.assertRaisesRegex(IOError, "Oh dear"):
            with obj:
                pass  # Do nothing

        self.assertFalse(getattr(obj, "_is_within_context"))

        with self.assertRaisesRegex(MustBeContextManagedError, "Must be context managed"):
            obj.call_inside_context()


class NonContextManagedClass:
    pass


@must_be_context_managed
class MustBeManaged(ContextManager["MustBeManaged"]):

    def __init__(self, string_var: str, num_var: int):
        self._string_var = string_var
        self._num_var = num_var
        self._enter_method_was_called = False
        self._exit_method_was_called = False

    def __enter__(self) -> "MustBeManaged":
        self._enter_method_was_called = True
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        self._exit_method_was_called = True

    def call_method_outside_context(self, a: int, b: str) -> bool:
        return len(b) > a

    @ensure_used_within_context_manager
    def call_method_inside_context(self, a: int, b: str) -> bool:
        return len(b) > a


class SubClass(MustBeManaged):

    def __enter__(self) -> "SubClass":
        super().__enter__()
        return self

    def subclass_method(self) -> str:
        return "from subclass"


class UndecoratedClass(ContextManager["UndecoratedClass"]):

    def __enter__(self) -> "UndecoratedClass":
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        pass

    @ensure_used_within_context_manager
    def call_a_method(self) -> str:
        return "Hello"


T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")


@must_be_context_managed
class GenericClass1(Generic[T1], ContextManager["GenericClass1[T1]"]):
    pass


@must_be_context_managed
class GenericClass3(Generic[T1, T2, T3], ContextManager["GenericClass3[T1, T2, T3]"]):
    pass


@must_be_context_managed
class CallsDecoratedMethodFromExit(ContextManager["CallsDecoratedMethodFromExit"]):

    def __enter__(self) -> "CallsDecoratedMethodFromExit":
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        self.cleanup()

    @ensure_used_within_context_manager
    def cleanup(self) -> None:
        pass  # Do nothing


@must_be_context_managed
class RaisesAnErrorDuringExit(ContextManager["RaisesAnErrorDuringExit"]):

    def __enter__(self) -> "RaisesAnErrorDuringExit":
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        raise IOError("Oh dear")

    @ensure_used_within_context_manager
    def call_inside_context(self) -> None:
        pass  # Do nothing
