from dataclasses import dataclass
from typing import Any, Callable, TypeVar

PoolType = TypeVar("PoolType")
PoolJob = Callable[[Any], PoolType]


@dataclass(frozen=True)
class PoolJobCall:
    job: PoolJob
    args: Any
