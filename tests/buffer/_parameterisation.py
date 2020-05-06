import time
from abc import abstractmethod
from typing import Any, List, TypeVar

from puma.environment import Environment, ProcessEnvironment, ThreadEnvironment
from puma.helpers.testing.parameterized import NamedTestParameters

Type = TypeVar("Type")
T = TypeVar("T")


class BufferTestEnvironment(Environment):
    @abstractmethod
    def publish_observe_delay(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_max_latency(self) -> float:
        raise NotImplementedError()

    @abstractmethod
    def get_max_mean_latency(self) -> float:
        raise NotImplementedError()

    @abstractmethod
    def get_min_throughput(self) -> float:
        raise NotImplementedError()


class ThreadBufferTestEnvironment(ThreadEnvironment, BufferTestEnvironment):
    def publish_observe_delay(self) -> None:
        pass  # Nothing to do

    def get_max_latency(self) -> float:
        return 0.02

    def get_max_mean_latency(self) -> float:
        return 0.005

    def get_min_throughput(self) -> float:
        return 300


class ProcessBufferTestEnvironment(ProcessEnvironment, BufferTestEnvironment):
    def publish_observe_delay(self) -> None:
        # Give the buried threads a chance to wake up and pump items through
        time.sleep(0.01)  # Our thread is event-driven so the only delay is for the ManagedProcessQueue to transfer a value, which is actually very quick

    def get_max_latency(self) -> float:
        return 0.05

    def get_max_mean_latency(self) -> float:
        return 0.005

    def get_min_throughput(self) -> float:
        return 100


class BufferTestParams(NamedTestParameters):
    def __init__(self, env: BufferTestEnvironment, options: Any = None) -> None:
        super().__init__(env.descriptive_name())
        self._env = env
        self._options = options


envs: List[BufferTestParams] = [
    BufferTestParams(ThreadBufferTestEnvironment()),
    BufferTestParams(ProcessBufferTestEnvironment())
]
