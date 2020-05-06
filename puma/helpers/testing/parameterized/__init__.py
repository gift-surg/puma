from typing import Any, Callable, Sequence

from parameterized import parameterized as parameterized_lib
from parameterized.parameterized import param, string_types

from puma.helpers.testing.parameterized.namedtestparameters import NamedTestParameters


def parameterized(test_case_parameters: Sequence[NamedTestParameters]) -> Any:
    """
    Parameterises a test function, ensuring that it gets a sensible / user defined name
    """
    return parameterized_lib.expand(test_case_parameters, name_func=_named_test_function)


def _named_test_function(func: Callable[[Any], Any], num: int, p: param) -> str:
    name_suffix = None
    if len(p.args) > 0:
        first_arg = p.args[0]
        if isinstance(first_arg, string_types):
            name_suffix = parameterized_lib.to_safe_name(first_arg)
        elif isinstance(first_arg, NamedTestParameters):
            name_suffix = parameterized_lib.to_safe_name(first_arg.name)
    return "_".join(filter(None, [func.__name__, str(num), name_suffix]))
