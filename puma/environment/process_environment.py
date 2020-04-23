import multiprocessing
import threading
from typing import Any, Callable, Iterable, Mapping, Optional, Type, TypeVar, Union

from puma.buffer import Buffer, MultiProcessBuffer
from puma.buffer.implementation.managed_queues import ManagedProcessQueue, ManagedQueueTypes
from puma.environment import Environment
from puma.primitives import ConditionType, EventType, ProcessCondition, ProcessEvent, ProcessSafeBool, ProcessSafeInt, SafeBoolType, SafeIntType
from puma.runnable import Runnable
from puma.runnable.runner import ProcessRunner, Runner

BT = TypeVar("BT")
QT = TypeVar("QT")


class ProcessEnvironment(Environment):
    def create_buffer(self, element_type: Type[BT], buffer_size: int, name: str, warn_on_discard: Optional[bool] = False) -> Buffer[BT]:
        return MultiProcessBuffer(buffer_size, name, warn_on_discard)

    def create_managed_queue(self, element_type: Type[QT], size: int = 0, name: Optional[str] = None) -> ManagedQueueTypes[QT]:
        return ManagedProcessQueue(size, name)

    def create_thread_or_process(self, name: str, target: Optional[Callable[..., Any]] = None, args: Optional[Iterable[Any]] = None, kwargs: Optional[Mapping[str, Any]] = None) \
            -> Union[threading.Thread, multiprocessing.Process]:
        return multiprocessing.Process(name=name, target=target, args=args or (), kwargs=kwargs or {})

    def create_event(self) -> EventType:
        return ProcessEvent()

    def create_condition(self) -> ConditionType:
        return ProcessCondition()

    def create_safe_int(self, initial_value: int) -> SafeIntType:
        return ProcessSafeInt(initial_value)

    def create_safe_bool(self, initial_value: bool) -> SafeBoolType:
        return ProcessSafeBool(initial_value)

    def create_runner(self, runnable: Runnable, name: Optional[str] = None) -> Runner:
        return ProcessRunner(runnable, name)

    def descriptive_name(self) -> str:
        return "MultiProcess"

    def runner_class_name(self) -> str:
        return "ProcessRunner"
