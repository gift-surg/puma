from abc import ABC
from typing import Any


class Mixin(ABC):
    """Base class for Mixins. Prevents MyPy from showing errors related to super().__init__(*args, **kwargs)"""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)  # type: ignore
