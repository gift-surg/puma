from puma.environment import Environment, ProcessEnvironment, ThreadEnvironment
from tests.parameterized import NamedTestParameters


class EnvironmentTestParameters(NamedTestParameters):
    def __init__(self, environment: Environment) -> None:
        super().__init__(get_name(environment))
        self.environment = environment


def get_name(environment: Environment) -> str:
    if isinstance(environment, ThreadEnvironment):
        return "MultiThreaded"
    elif isinstance(environment, ProcessEnvironment):
        return "MultiProcess"
    else:
        raise RuntimeError(f"Unknown environment: {environment}")


environments = [
    EnvironmentTestParameters(ThreadEnvironment()),
    EnvironmentTestParameters(ProcessEnvironment())
]
