import json
from typing import Callable, List
from unittest import TestCase

from puma.buffer import Publishable
from puma.scope_id import get_current_scope_id
from tests.runnable.proxy.proxy_simple_test_case import SendsCallsToBufferRunnable, SimpleProxyTestCase
from tests.runnable.proxy.proxy_test_case import RunnableTestInterface
from tests.runnable.proxy.proxy_test_environment import ProxyTestEnvironment
from tests.runnable.proxy.proxy_test_helpers import AllMethodsReturnNone, CallResponse, SendsCallsToBufferImpl


class RaisesErrorProxyTestCase(SimpleProxyTestCase):

    def create_demo_interface(self, call_response_publishable: Publishable[CallResponse]) -> RunnableTestInterface:
        return RaisesErrorSendsCallsToBufferRunnable(SendsCallsToBufferImpl(call_response_publishable))

    def perform_commands(self, test_case: TestCase, test_interface: AllMethodsReturnNone) -> None:
        self._assert_error_raised_for_method(test_case, test_interface, lambda t: t.no_args(), "no_args")
        self._assert_error_raised_for_method(test_case, test_interface, lambda t: t.one_arg("hello"), "one_arg")
        self._assert_error_raised_for_method(test_case, test_interface, lambda t: t.two_args("hello", "bye"), "two_args")

    def _assert_error_raised_for_method(self, test_case: TestCase, test_interface: AllMethodsReturnNone, action: Callable[[AllMethodsReturnNone], None], method_name: str) -> None:
        with test_case.assertRaises(RuntimeError) as detailed:
            action(test_interface)
        error_message_obj = json.loads(str(detailed.exception))
        # Ensure the correct method was called
        test_case.assertEqual(method_name, error_message_obj["method"])
        # Ensure that it was called in a remote scope (the call was actually proxied!)
        test_case.assertNotEqual(get_current_scope_id(), error_message_obj["scope"])

    def check_results(self, test_case: TestCase, proxy_test_env: ProxyTestEnvironment, commands: List[CallResponse]) -> None:
        # Do nothing
        pass

    def _get_expected_command_count(self) -> int:
        return 0


class RaisesErrorSendsCallsToBufferRunnable(SendsCallsToBufferRunnable, AllMethodsReturnNone):

    def no_args(self) -> None:
        raise self.__error("no_args")

    def one_arg(self, a: str) -> None:
        raise self.__error("one_arg")

    def two_args(self, a: str, b: str) -> None:
        raise self.__error("two_args")

    def __error(self, method_name: str) -> RuntimeError:
        return RuntimeError(json.dumps({"method": method_name, "scope": get_current_scope_id()}))
