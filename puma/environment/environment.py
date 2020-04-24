import multiprocessing
import threading
from abc import ABC, abstractmethod
from typing import Any, Callable, Iterable, Mapping, Optional, Type, TypeVar, Union

from puma.buffer import Buffer
from puma.buffer.implementation.managed_queues import ManagedQueueTypes
from puma.primitives import ConditionType, EventType, SafeBoolType, SafeIntType
from puma.runnable import Runnable
from puma.runnable.runner import Runner

BT = TypeVar("BT")
QT = TypeVar("QT")


class Environment(ABC):
    @abstractmethod
    def create_buffer(self, element_type: Type[BT], size: int, name: str, warn_on_discard: Optional[bool] = False) -> Buffer[BT]:
        raise NotImplementedError()

    @abstractmethod
    def create_managed_queue(self, element_type: Type[QT], size: int = 0, name: Optional[str] = None) -> ManagedQueueTypes[QT]:
        raise NotImplementedError()

    @abstractmethod
    def create_thread_or_process(self, name: str, target: Optional[Callable[..., Any]] = None, args: Optional[Iterable[Any]] = None, kwargs: Optional[Mapping[str, Any]] = None) \
            -> Union[threading.Thread, multiprocessing.Process]:
        raise NotImplementedError()

    @abstractmethod
    def create_event(self) -> EventType:
        raise NotImplementedError()

    @abstractmethod
    def create_condition(self) -> ConditionType:
        raise NotImplementedError()

    @abstractmethod
    def create_safe_int(self, initial_value: int) -> SafeIntType:
        raise NotImplementedError()

    @abstractmethod
    def create_safe_bool(self, initial_value: bool) -> SafeBoolType:
        raise NotImplementedError()

    @abstractmethod
    def create_runner(self, runnable: Runnable, name: Optional[str] = None) -> Runner:
        raise NotImplementedError()

    @abstractmethod
    def descriptive_name(self) -> str:
        raise NotImplementedError()

    @abstractmethod
    def runner_class_name(self) -> str:
        raise NotImplementedError()
