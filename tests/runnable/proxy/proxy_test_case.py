from abc import ABC, abstractmethod
from typing import List
from unittest import TestCase

from puma.buffer import Publishable
from puma.runnable import Runnable
from tests.runnable.proxy.proxy_test_environment import ProxyTestEnvironment
from tests.runnable.proxy.proxy_test_helpers import CallResponse, HasMethodThatReturnsValue


class RunnableTestInterface(Runnable, HasMethodThatReturnsValue):
    pass


class ProxyTestCase(ABC):

    @abstractmethod
    def create_demo_interface(self, call_response_publishable: Publishable[CallResponse]) -> RunnableTestInterface:
        raise NotImplementedError()

    @abstractmethod
    def perform_commands(self, test_case: TestCase, test_interface: HasMethodThatReturnsValue) -> None:
        raise NotImplementedError()

    @abstractmethod
    def check_results(self, test_case: TestCase, proxy_test_env: ProxyTestEnvironment, commands: List[CallResponse]) -> None:
        raise NotImplementedError()
