from typing import Any


def get_fully_qualified_name(o: Any) -> str:
    """Returns the fully qualified class name of the given object as a string."""
    return get_class_fully_qualified_name(o.__class__)


def get_class_fully_qualified_name(t: Any) -> str:
    """Returns the fully qualified class name of the given class as a string."""
    module = t.__module__
    if module is None or module == str.__class__.__module__:
        return t.__qualname__  # type: ignore
    else:
        return module + '.' + t.__qualname__  # type: ignore
