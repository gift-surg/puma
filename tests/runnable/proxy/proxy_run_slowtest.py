from abc import ABC
from typing import List, Type
from unittest import TestCase

from puma.attribute import child_only, child_scope_value
from puma.environment import ProcessEnvironment, ThreadEnvironment
from puma.helpers.testing.parameterized import parameterized
from puma.runnable import CommandDrivenRunnable
from puma.runnable.decorator.run_in_child_scope import run_in_child_scope
from puma.runnable.proxy import Proxy
from puma.runnable.remote_execution import BaseRemoteObjectReference
from tests.runnable.proxy.proxy_complex_test_case import ComplexObjectProxyTestCase
from tests.runnable.proxy.proxy_creation_test import get_all_items_from_buffer
from tests.runnable.proxy.proxy_error_test_case import RaisesErrorProxyTestCase
from tests.runnable.proxy.proxy_simple_test_case import SimpleProxyTestCase
from tests.runnable.proxy.proxy_test_case import ProxyTestCase
from tests.runnable.proxy.proxy_test_environment import ProxyTestEnvironment
from tests.runnable.proxy.proxy_test_helpers import AllMethodsReturnNone, CallResponse, DIFFERENT_PROCESS, DIFFERENT_THREAD, HasMethodThatReturnsComplexObject, \
    HasMethodThatReturnsValue

envs = [
    ProxyTestEnvironment(ProcessEnvironment(), ProcessEnvironment(), DIFFERENT_PROCESS),
    ProxyTestEnvironment(ProcessEnvironment(), ThreadEnvironment(), DIFFERENT_PROCESS),
    ProxyTestEnvironment(ThreadEnvironment(), ProcessEnvironment(), DIFFERENT_PROCESS),
    ProxyTestEnvironment(ThreadEnvironment(), ThreadEnvironment(), DIFFERENT_THREAD)
]


class ProxyRunTest(TestCase):

    @parameterized(envs)
    def test_it_proxies_method_that_calls_and_waits_for_result(self, env: ProxyTestEnvironment) -> None:
        self._verify_simple_test_case(env, HasMethodThatReturnsValue, SimpleProxyTestCase("Called by SendsCallsToBufferRunnable"))

    @parameterized(envs)
    def test_it_proxies_method_that_calls_and_waits_for_result_with_manual_implementation(self, env: ProxyTestEnvironment) -> None:
        self._verify_simple_test_case(env, PartialProxyThatImplementsMethodByDelegatingToOriginalMethod, SimpleProxyTestCase("Called by SendsCallsToBufferRunnable"))

    @parameterized(envs)
    def test_it_proxies_methods_correctly_with_custom_override(self, env: ProxyTestEnvironment) -> None:
        self._verify_simple_test_case(env, PartialProxyThatImplementsMethodByDelegatingToOriginalMethodThenReturningOwnValue,
                                      SimpleProxyTestCase("own value: Called by SendsCallsToBufferRunnable"))

    @parameterized(envs)
    def test_it_proxies_methods_correctly_that_return_complex_objects(self, env: ProxyTestEnvironment) -> None:
        self._verify_simple_test_case(env, HasMethodThatReturnsComplexObject, ComplexObjectProxyTestCase("Called by ComplexSendsCallsToBufferRunnable"))

    @parameterized(envs)
    def test_it_proxies_methods_correctly_that_raise_an_error(self, env: ProxyTestEnvironment) -> None:
        self._verify_simple_test_case(env, AllMethodsReturnNone, RaisesErrorProxyTestCase("Called by ComplexSendsCallsToBufferRunnable"))

    def _verify_simple_test_case(self, env: ProxyTestEnvironment, partial_interface_impl: Type, proxy_test_case: ProxyTestCase) -> None:
        commands = self._perform_simple_test_case(env, partial_interface_impl, proxy_test_case)

        proxy_test_case.check_results(self, env, commands)

    # Type info for partial_interface_impl cannot be more specific - see: https://github.com/python/mypy/issues/5374
    def _perform_simple_test_case(self, env: ProxyTestEnvironment, partial_interface_impl: Type, proxy_test_case: ProxyTestCase) -> List[CallResponse]:

        with env.source_env.create_buffer(CallResponse, 100, "Call Response Buffer") as call_response_buffer:
            demo_interface = proxy_test_case.create_demo_interface(call_response_buffer)
            with Proxy(partial_interface_impl, demo_interface) as proxy:
                facade: HasMethodThatReturnsValue = proxy.get_facade()

                controlling_runnable = CallInterfaceRunnable("Call Interface Runnable", TestCase(), facade, proxy_test_case)

                with proxy.get_runner() as proxy_runner, \
                        env.source_env.create_runner(demo_interface, "Source Runner") as source_runner, \
                        env.calling_env.create_runner(controlling_runnable, "Calling Runner") as controlling_runner:
                    runners = [proxy_runner, source_runner]

                    for runner in runners:
                        runner.start()

                    for runner in runners:
                        runner.wait_until_running()

                    controlling_runner.start_blocking()
                    controlling_runnable.perform_commands()
                    controlling_runner.join()

                    all_items = get_all_items_from_buffer(call_response_buffer)

                    for runner in [*runners, controlling_runner]:
                        runner.stop()

                    for runner in [*runners, controlling_runner]:
                        runner.join()

                    return all_items


class CallInterfaceRunnable(CommandDrivenRunnable):
    _test_case: TestCase = child_only("_test_case")
    _test_interface: HasMethodThatReturnsValue = child_only("_test_interface")
    _proxy_test_case: ProxyTestCase = child_only("_proxy_test_case")

    def __init__(self, name: str, test_case: TestCase, test_interface: HasMethodThatReturnsValue, proxy_test_case: ProxyTestCase) -> None:
        super().__init__(name, [])
        self._test_case = child_scope_value(test_case)
        self._test_interface = child_scope_value(test_interface)
        self._proxy_test_case = child_scope_value(proxy_test_case)

    @run_in_child_scope
    def perform_commands(self) -> None:
        self._proxy_test_case.perform_commands(self._test_case, self._test_interface)
        self._handle_stop_command()


class PartialProxyThatImplementsMethodByDelegatingToOriginalMethodThenReturningOwnValue(BaseRemoteObjectReference[HasMethodThatReturnsValue], HasMethodThatReturnsValue, ABC):

    def returns_value(self, a: str, b: int) -> str:
        original_value = self._remote_method(self._wrapped_instance.returns_value).call(a, b)
        return f"own value: {original_value}"


class PartialProxyThatImplementsMethodByDelegatingToOriginalMethod(BaseRemoteObjectReference[HasMethodThatReturnsValue], HasMethodThatReturnsValue, ABC):

    def returns_value(self, a: str, b: int) -> str:
        return self._remote_method(self._wrapped_instance.returns_value).call(a, b)
