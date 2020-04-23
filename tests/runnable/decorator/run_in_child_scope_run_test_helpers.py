from dataclasses import dataclass, field
from time import sleep
from typing import Callable, Optional

from puma.context import ContextManager, Exit_1, Exit_2, Exit_3, MustBeContextManagedError, must_be_context_managed
from puma.runnable import CommandDrivenRunnable, Runnable
from puma.runnable.decorator.run_in_child_scope import run_in_child_scope
from puma.runnable.message import StatusMessage
from puma.runnable.runner import Runner
from puma.scope_id import get_current_scope_id
from tests.parameterized import NamedTestParameters


class RunnableTestClass(CommandDrivenRunnable):

    def __init__(self) -> None:
        super().__init__("RunInChildScope Test Runnable", [])

    def undecorated_method(self) -> str:
        return get_current_scope_id()

    def _get_current_scope_id(self) -> str:
        return get_current_scope_id()

    def local_get_current_scope_id(self) -> str:
        return self._get_current_scope_id()

    @run_in_child_scope
    def remote_get_current_scope_id(self) -> str:
        return self._get_current_scope_id()

    @run_in_child_scope
    def no_args(self) -> int:
        self._send_status_message(RunnableTestClassStatusMessage("no_args()"))
        return 102

    @run_in_child_scope
    def some_args(self, a: int, b: str = "default") -> int:
        self._send_status_message(RunnableTestClassStatusMessage(f"some_args({a}, {b})"))
        return a * len(b)

    @run_in_child_scope
    def sync_method(self, sleep_duration: float) -> float:
        self._send_status_message(RunnableTestClassStatusMessage(f"sync_method({sleep_duration})"))
        sleep(sleep_duration)
        return sleep_duration


class RunnableTestSubClass(RunnableTestClass):

    @run_in_child_scope
    def no_args(self) -> int:
        self._send_status_message(RunnableTestClassStatusMessage("child:no_args()"))
        return 502

    @run_in_child_scope
    def some_args(self, a: int, b: str = "default") -> int:
        self._send_status_message(RunnableTestClassChildStatusMessage(f"child:some_args({a}, {b})"))
        sleep(0.1)
        return a * super().some_args(a, b)


class ComplexObject:

    def __init__(self, a: int, b: str) -> None:
        self._a = a
        self._b = b

    def get_a(self) -> int:
        return self._a

    def get_b(self) -> str:
        return self._b

    def get_sub_complex(self) -> "SubComplexObject":
        return SubComplexObject()


@dataclass
class SubComplexResponseObject2:
    attr_1: str
    attr_2: int


@dataclass(frozen=True)
class SubComplexResponseObject:
    primitive: int
    non_primitive: SubComplexResponseObject2


class SubComplexObject:

    def get_scope_id(self) -> str:
        return get_current_scope_id()

    def returns_non_primitive(self, param_1: int, param_2: str, param_3: int) -> SubComplexResponseObject:
        return SubComplexResponseObject(param_1, SubComplexResponseObject2(param_2, param_3))


class RunnableTestComplexClass(RunnableTestClass):

    @run_in_child_scope
    def get_complex_object(self, a: int, b: str) -> ComplexObject:
        return ComplexObject(a, b)


@must_be_context_managed
class Parameters(NamedTestParameters, ContextManager["Parameters"]):

    def __init__(self, name: str, runner_factory: Callable[[Runnable], Runner]) -> None:
        super().__init__(name)
        self._runner_factory = runner_factory
        self._runnable: Optional[RunnableTestClass] = None

    def __enter__(self) -> "Parameters":
        # Ensure a new Runnable is created for each test case
        self._runnable = RunnableTestClass()
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        super().__exit__(exc_type, exc_value, traceback)

    @property
    def default_runnable(self) -> RunnableTestClass:
        if self._runnable:
            return self._runnable
        else:
            raise MustBeContextManagedError()

    def create_runner(self, runnable: Optional[Runnable] = None) -> Runner:
        if not runnable:
            runnable = self.default_runnable
        return self._runner_factory(runnable)


@dataclass(frozen=True)
class RunnableTestClassStatusMessage(StatusMessage):
    method: str
    scope_id: str = field(init=False, default_factory=get_current_scope_id)


@dataclass(frozen=True)
class RunnableTestClassChildStatusMessage(RunnableTestClassStatusMessage):
    pass
