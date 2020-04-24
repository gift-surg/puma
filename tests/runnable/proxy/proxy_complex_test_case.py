from typing import List, cast
from unittest import TestCase

from puma.buffer import Publishable
from puma.runnable.decorator.run_in_child_scope import run_in_child_scope
from puma.scope_id import get_current_scope_id
from tests.runnable.proxy.proxy_simple_test_case import SendsCallsToBufferRunnable, SimpleProxyTestCase
from tests.runnable.proxy.proxy_test_case import RunnableTestInterface
from tests.runnable.proxy.proxy_test_environment import ProxyTestEnvironment
from tests.runnable.proxy.proxy_test_helpers import CallResponse, ComplexObject, HasMethodThatReturnsComplexObject, HasMethodThatReturnsValue, SendsCallsToBufferImpl


class ComplexObjectProxyTestCase(SimpleProxyTestCase):

    def create_demo_interface(self, call_response_publishable: Publishable[CallResponse]) -> RunnableTestInterface:
        return ComplexSendsCallsToBufferRunnable(ComplexSendCallsToBufferImpl(call_response_publishable))

    def perform_commands(self, test_case: TestCase, test_interface: HasMethodThatReturnsValue) -> None:
        super().perform_commands(test_case, test_interface)

        impl = cast(HasMethodThatReturnsComplexObject, test_interface)
        complex_object = impl.get_complex_object(get_current_scope_id(), 45, False)

        # Ensure calling methods on complex object doesn't raise any errors
        complex_object.get_str_value()
        complex_object.get_number()
        complex_object.get_bool()

        with test_case.assertRaisesRegex(TypeError, "too many positional arguments"):
            # noinspection PyArgumentList
            complex_object.get_str_value("with an arg")  # type: ignore

    def check_results(self, test_case: TestCase, proxy_test_env: ProxyTestEnvironment, commands: List[CallResponse]) -> None:
        super().check_results(test_case, proxy_test_env, commands)

        test_case.assertEqual("get_complex_object", commands[5].method_name)
        test_case.assertEqual(45, commands[5].args[1])
        test_case.assertEqual(False, commands[5].args[2])

    def _get_expected_command_count(self) -> int:
        return 6


class ComplexSendsCallsToBufferRunnable(SendsCallsToBufferRunnable, HasMethodThatReturnsComplexObject):

    def get_complex_object(self, str_value: str, number: int, boolean: bool) -> ComplexObject:
        self._in_child_get_complex_object(str_value, number, boolean)
        prefix = f"From {self.__class__.__name__}"
        return ComplexObject(f"{prefix} - {str_value}", number, boolean)

    @run_in_child_scope
    def _in_child_get_complex_object(self, str_value: str, number: int, boolean: bool) -> None:
        cast(HasMethodThatReturnsComplexObject, self._wrapped_instance).get_complex_object(str_value, number, boolean)


class ComplexSendCallsToBufferImpl(SendsCallsToBufferImpl, HasMethodThatReturnsComplexObject):

    def get_complex_object(self, str_value: str, number: int, boolean: bool) -> ComplexObject:
        with self._feedback_publishable.publish() as publisher:
            publisher.publish_value(CallResponse("get_complex_object", [str_value, number, boolean]))
        return ComplexObject(str_value, number, boolean)
