from typing import Any, List, TypeVar, Union

from puma.environment import ProcessEnvironment, ThreadEnvironment
from tests.parameterized.namedtestparameters import NamedTestParameters

Type = TypeVar("Type")


class StatusBufferTestParams(NamedTestParameters):
    def __init__(self, env: Union[ThreadEnvironment, ProcessEnvironment], options: Any = None) -> None:
        super().__init__(env.descriptive_name())
        self._env = env
        self._options = options


envs: List[StatusBufferTestParams] = [
    StatusBufferTestParams(ThreadEnvironment()),
    StatusBufferTestParams(ProcessEnvironment())
]
