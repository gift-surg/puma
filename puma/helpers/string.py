from typing import Any, List, Optional


def safe_str(x: Any) -> str:
    """Useful utility in place of str() to handle None and other errors in logging without breaking the program."""
    try:
        if x is None:
            return 'None'
        if isinstance(x, BaseException):
            return repr(x)
        return str(x)
    except Exception as ex:
        return '<ERROR>: ' + str(ex)


def list_str(lst: Optional[List[Any]]) -> str:
    """To-string for a list, calls safe_str() on each element, unless that is a string in which case it is quoted."""

    def stringify(x: Any) -> str:
        if isinstance(x, str):
            return "'" + x + "'"
        else:
            return safe_str(x)

    if lst is None:
        return 'None'
    else:
        return "[" + ", ".join([stringify(el) for el in lst]) + "]"
