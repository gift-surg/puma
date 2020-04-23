import warnings
from enum import Enum
from typing import Any, Optional, Type


class EnumMemberError(TypeError):
    """Exception to be raised when a passed value is not a member of an enumeration."""

    def __init__(self, msg: str) -> None:
        super().__init__()
        self._msg = msg

    def __str__(self) -> str:
        return self._msg


def ensure_enum_member(value: Optional[Any], enum_class: Type[Enum]) -> None:
    """Raise an EnumMemberError if passed value is not in given enum class."""

    # Python 3.8 compatibility, see https://docs.python.org/3/whatsnew/3.7.html#id5
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=DeprecationWarning)
        try:
            if value is None or value not in enum_class:
                raise EnumMemberError(f'{value} is not in {enum_class}')
        except TypeError as e:
            raise EnumMemberError(str(e))
