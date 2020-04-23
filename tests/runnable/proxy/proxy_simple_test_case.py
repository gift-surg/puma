from typing import List
from unittest import TestCase

from puma.attribute import copied
from puma.buffer import Publishable
from puma.runnable import CommandDrivenRunnable
from puma.runnable.decorator.run_in_child_scope import run_in_child_scope
from puma.scope_id import get_current_scope_id
from tests.runnable.proxy.proxy_test_case import ProxyTestCase, RunnableTestInterface
from tests.runnable.proxy.proxy_test_environment import ProxyTestEnvironment
from tests.runnable.proxy.proxy_test_helpers import CallResponse, HasMethodThatReturnsValue, SendsCallsToBufferImpl, from_scope_id


class SimpleProxyTestCase(ProxyTestCase):

    def __init__(self, expected_result_value: str):
        self._expected_result_value = expected_result_value

    def create_demo_interface(self, call_response_publishable: Publishable[CallResponse]) -> RunnableTestInterface:
        return SendsCallsToBufferRunnable(SendsCallsToBufferImpl(call_response_publishable))

    def perform_commands(self, test_case: TestCase, test_interface: HasMethodThatReturnsValue) -> None:
        test_interface.no_args()
        test_interface.one_arg(get_current_scope_id())
        test_interface.two_args(get_current_scope_id(), "2")
        value = test_interface.returns_value(get_current_scope_id(), 3)
        test_interface.two_args(get_current_scope_id(), value)

    def check_results(self, test_case: TestCase, proxy_test_env: ProxyTestEnvironment, commands: List[CallResponse]) -> None:

        test_case.assertEqual(self._get_expected_command_count(), len(commands))

        # Ensure the correct commands were called (can't verify first argument, as it is generated - it will be checked later)
        test_case.assertEqual("no_args", commands[0].method_name)
        test_case.assertEqual([], commands[0].args)

        test_case.assertEqual("one_arg", commands[1].method_name)

        test_case.assertEqual("two_args", commands[2].method_name)
        test_case.assertEqual("2", commands[2].args[1])

        test_case.assertEqual("returns_value", commands[3].method_name)
        test_case.assertEqual(3, commands[3].args[1])

        test_case.assertEqual("two_args", commands[4].method_name)
        test_case.assertEqual(self._expected_result_value, commands[4].args[1])

        # Ensure all commands ran in the same scope
        command_run_scope_ids = set()
        for c in commands:
            command_run_scope_ids.add(c.scope_id)
        test_case.assertEqual(1, len(command_run_scope_ids), f"Not all commands were run in the same scope - {command_run_scope_ids}")

        # Ensure all commands called in the same scope
        command_called_scope_ids = set()
        for c in commands:
            if len(c.args) > 0:
                command_called_scope_ids.add(c.args[0])
        test_case.assertEqual(1, len(command_called_scope_ids), f"Not all commands were called in the same scope - {command_called_scope_ids}")

        command_called_scope = from_scope_id(command_called_scope_ids.pop())
        command_run_scope = from_scope_id(command_run_scope_ids.pop())

        # Ensure commands weren't called from or run in the main thread
        main_thread_scope = from_scope_id(get_current_scope_id())
        test_case.assertNotEqual(main_thread_scope, command_called_scope)
        test_case.assertNotEqual(main_thread_scope, command_run_scope)

        # Ensure commands were called from the expected scope
        proxy_test_env.environment_verifier.verify(test_case, command_called_scope, command_run_scope)

    def _get_expected_command_count(self) -> int:
        return 5


class SendsCallsToBufferRunnable(CommandDrivenRunnable, RunnableTestInterface):
    _wrapped_instance: HasMethodThatReturnsValue = copied("_wrapped_instance")

    def __init__(self, wrapped_interface: HasMethodThatReturnsValue) -> None:
        super().__init__(self.__class__.__name__, [])
        self._wrapped_instance = wrapped_interface

    @run_in_child_scope
    def no_args(self) -> None:
        self._wrapped_instance.no_args()

    @run_in_child_scope
    def one_arg(self, a: str) -> None:
        self._wrapped_instance.one_arg(a)

    @run_in_child_scope
    def two_args(self, a: str, b: str) -> None:
        self._wrapped_instance.two_args(a, b)

    def returns_value(self, a: str, b: int) -> str:
        self._in_child_returns_value(a, b)
        return f"Called by {self.__class__.__name__}"

    @run_in_child_scope
    def _in_child_returns_value(self, a: str, b: int) -> None:
        self._wrapped_instance.returns_value(a, b)
