from dataclasses import dataclass
from enum import Enum, auto
from typing import Any

from puma.attribute.attribute.late_init import late_init


class DetectedScopeType(Enum):
    parent = auto()
    child = auto()


def get_parent_const() -> "DetectedScope":
    return DetectedScope.create_parent_instance("")


def get_child_const() -> "DetectedScope":
    return DetectedScope.create_child_instance("")


@dataclass
class DetectedScope:
    scope_type: DetectedScopeType
    current_scope_id: str

    parent = late_init(get_parent_const)
    child = late_init(get_child_const)

    @staticmethod
    def create_parent_instance(current_scope_id: str) -> "DetectedScope":
        return DetectedScope(DetectedScopeType.parent, current_scope_id)

    @staticmethod
    def create_child_instance(current_scope_id: str) -> "DetectedScope":
        return DetectedScope(DetectedScopeType.child, current_scope_id)

    @property
    def name(self) -> str:
        return self.scope_type.name

    def __eq__(self, other: Any) -> bool:
        """Ignore current_scope_id when comparing DetectedScope equality"""
        if isinstance(other, DetectedScope):
            return self.scope_type == other.scope_type
        else:
            return False
