from abc import abstractmethod
from typing import Any, List, Union

from puma.environment import ProcessEnvironment, ThreadEnvironment
from tests.parameterized.namedtestparameters import NamedTestParameters


class RunnerTestEnvironment:
    @abstractmethod
    def activity_timeout(self) -> float:
        raise NotImplementedError()


class ThreadRunnerTestEnvironment(ThreadEnvironment, RunnerTestEnvironment):
    def activity_timeout(self) -> float:
        return 0.5


class ProcessRunnerTestEnvironment(ProcessEnvironment, RunnerTestEnvironment):
    def activity_timeout(self) -> float:
        return 10.0


class RunnerTestParams(NamedTestParameters):
    def __init__(self, env: Union[ThreadRunnerTestEnvironment, ProcessRunnerTestEnvironment], options: Any = None) -> None:
        super().__init__(env.descriptive_name())
        self._env = env
        self._options = options


envs: List[RunnerTestParams] = [
    RunnerTestParams(ThreadRunnerTestEnvironment()),
    RunnerTestParams(ProcessRunnerTestEnvironment())
]
