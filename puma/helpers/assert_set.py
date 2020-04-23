from typing import Optional, TypeVar

T = TypeVar("T")


def assert_set(value: Optional[T], value_name: Optional[str] = None, error_message: Optional[str] = None) -> T:
    if value is not None:
        return value
    else:
        if error_message is None:
            value_name = value_name or "Value"
            error_message = f"{value_name} is not set"
        raise AssertionError(error_message)
