from abc import ABC
from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class CommandMessage(ABC):
    """Base class for Command messages sent to a runnable. Classes are used so that in future, message-specific data can be added to a message if required."""


@dataclass(frozen=True)
class StopCommandMessage(CommandMessage):
    """Message requesting that the Runnable stop"""


@dataclass(frozen=True)
class RunInChildScopeCommandMessage(CommandMessage):
    """Message containing name and arguments for delegated method (decorated with @run_in_child_scope)"""
    call_id: str
    name: str
    remote_object_id: Optional[str]


@dataclass(frozen=True)
class RemoteObjectMethodCommandMessage(RunInChildScopeCommandMessage):
    args: Any
    kwargs: Any


@dataclass(frozen=True)
class RemoteObjectGetAttributeCommandMessage(RunInChildScopeCommandMessage):
    pass
