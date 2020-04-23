from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True)
class ScopedAttributeState:
    attributes: Dict[str, Any]
