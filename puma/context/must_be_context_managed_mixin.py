import functools
from typing import Any, Callable, TypeVar, cast

from puma.context import ContextManager
from puma.mixin import Mixin

DecoratedFunction = Callable[..., Any]

DecoratedFunctionType = TypeVar("DecoratedFunctionType", bound=DecoratedFunction)


def get_self(args: Any) -> "MustBeContextManagedMixin":
    return cast(MustBeContextManagedMixin, args[0])


class MustBeContextManagedMixin(ContextManager[Any], Mixin):
    """
    A mixin that tracks whether or not the instance is currently being context managed
    Note: It must not be used directly - please use the @must_be_context_managed decorator to apply it
    """
    _is_within_context: bool

    @staticmethod
    def init_wrapper(init_method: DecoratedFunctionType) -> DecoratedFunctionType:
        functools.wraps(init_method)

        def wrap(*args: Any, **kwargs: Any) -> Any:
            get_self(args)._is_within_context = False
            # Ignore type below, as function is callable but MyPy can't work that out
            return init_method.__call__(*args, **kwargs)  # type: ignore

        return cast(DecoratedFunctionType, wrap)

    @staticmethod
    def enter_wrapper(enter_method: DecoratedFunctionType) -> DecoratedFunctionType:
        functools.wraps(enter_method)

        def wrap(*args: Any, **kwargs: Any) -> Any:
            get_self(args)._is_within_context = True
            # Ignore type below, as function is callable but MyPy can't work that out
            return enter_method.__call__(*args, **kwargs)  # type: ignore

        return cast(DecoratedFunctionType, wrap)

    @staticmethod
    def exit_wrapper(exit_method: DecoratedFunctionType) -> DecoratedFunctionType:
        functools.wraps(exit_method)

        def wrap(*args: Any, **kwargs: Any) -> Any:
            try:
                # Ignore type below, as function is callable but MyPy can't work that out
                result = exit_method.__call__(*args, **kwargs)  # type: ignore
            finally:
                get_self(args)._is_within_context = False
            return result

        return cast(DecoratedFunctionType, wrap)
