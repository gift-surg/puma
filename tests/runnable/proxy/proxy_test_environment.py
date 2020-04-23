from puma.environment import Environment
from tests.parameterized import NamedTestParameters
from tests.runnable.proxy.proxy_test_helpers import EnvironmentVerifier


class ProxyTestEnvironment(NamedTestParameters):

    def __init__(self, source_env: Environment, calling_env: Environment, environment_verifier: EnvironmentVerifier):
        super().__init__(f"Source: {source_env.__class__.__name__}; Calling: {calling_env.__class__.__name__}")
        self.source_env = source_env
        self.calling_env = calling_env
        self.environment_verifier = environment_verifier
