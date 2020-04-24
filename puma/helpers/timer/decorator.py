import functools
from typing import Any, Callable, Optional, TypeVar, Union, cast

from puma.helpers.timer import Timer

DecoratedFunction = Callable[..., Any]
DecoratedFunctionType = TypeVar("DecoratedFunctionType", bound=DecoratedFunction)

OutputMethod = Callable[[str], None]
DescriptionFactory = Callable[[Callable], str]
Description = Union[str, DescriptionFactory]


class DecoratedFunctionWithArguments:

    @staticmethod
    def __call__(_method: Optional[DecoratedFunctionType], *args: Any, **kwargs: Any) -> DecoratedFunctionType:
        pass


def time(*, output_method: Optional[OutputMethod] = None, description: Optional[Description] = None) -> DecoratedFunctionWithArguments:
    """
    A decorator that allows the time a method takes to execute to be measured and recorded

    Note: This decorator must be used WITH parentheses, eg

    @time()
    def my_method(): ...

    """

    def wrapper(method: DecoratedFunctionType) -> DecoratedFunctionType:
        @functools.wraps(method)
        def call(*args: Any, **kwargs: Any) -> Any:
            _output_method = output_method or print
            _description = description or method.__name__

            if callable(_description):
                _description = _description(method)

            with Timer() as timer:
                result = method(*args, **kwargs)
                _output_method(f"{_description} took {timer.get_elapsed_time():.2f} seconds")

            return result

        return cast(DecoratedFunctionType, call)

    return cast(DecoratedFunctionWithArguments, wrapper)
