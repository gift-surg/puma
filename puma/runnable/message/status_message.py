import types
from abc import ABC
from dataclasses import dataclass
from typing import Generic, Type, TypeVar, cast


@dataclass(frozen=True)
class StatusMessage(ABC):
    """Base class for event items. Classes are used so that in future, event-specific data can be added to an event if required."""


@dataclass(frozen=True)
class StartedStatusMessage(StatusMessage):
    """Event item containing the Started event"""


R = TypeVar("R")


@dataclass(frozen=True)
class RunInChildScopeStatusMessage(Generic[R], StatusMessage):
    """Message containing result from a delegated method (decorated with @run_in_child_scope)"""
    call_id: str
    result: R


MT = TypeVar("MT", bound=StatusMessage)


def status_message_type(name: str, target_type: Type[MT]) -> Type[MT]:
    return cast(Type[MT], types.new_class(name, (target_type,), {}))
