import functools
from contextlib import AbstractContextManager
from typing import Any, Callable, ContextManager, Type, TypeVar, cast

from puma.context import ContextManager as CustomContextManager, MustBeContextManagedError
from puma.context.must_be_context_managed_mixin import MustBeContextManagedMixin

DecoratedFunction = Callable[..., Any]

DecoratedFunctionType = TypeVar("DecoratedFunctionType", bound=DecoratedFunction)
DecoratedClassType = TypeVar("DecoratedClassType", bound=Type[ContextManager[Any]])


def _must_be_context_managed(class_: DecoratedClassType) -> DecoratedClassType:
    """A decorator that marks a class as requiring context management for proper usage"""
    # Check whether the class is a ContextManager
    # Can't use issubclass(clazz, ContextManager) here, as it erroneously returns True in some cases
    if AbstractContextManager not in class_.__bases__ and CustomContextManager not in class_.__bases__:
        raise RuntimeError("must_be_context_managed decorator may only be used on a subclass of typing.ContextManger, ideally puma.context.ContextManager")

    # Apply Mixin to class
    class_.__bases__ = (MustBeContextManagedMixin, *class_.__bases__,)

    # Wrap __init__, __enter__ and __exit__ methods
    setattr(class_, "__init__", MustBeContextManagedMixin.init_wrapper(class_.__init__))
    setattr(class_, "__enter__", MustBeContextManagedMixin.enter_wrapper(class_.__enter__))
    setattr(class_, "__exit__", MustBeContextManagedMixin.exit_wrapper(class_.__exit__))

    return class_


def _ensure_used_within_context_manager(function: DecoratedFunctionType) -> DecoratedFunctionType:
    """A decorator that marks a method as being required to be called within a context manager block"""

    @functools.wraps(function)
    def wrapper(self: MustBeContextManagedMixin, *args: Any, **kwargs: Any) -> Any:
        if not isinstance(self, MustBeContextManagedMixin):
            raise RuntimeError("Unable to determine if instance has been context managed - is the class annotated with @must_be_context_managed?")

        # Only check if we're within the context if the tracking variable exists (if it doesn't, we're almost certainly in a child context where we don't care about it)
        if hasattr(self, "_is_within_context"):
            if not self._is_within_context:
                raise MustBeContextManagedError()

        # Ignore type below, as function is callable but MyPy can't work that out
        return function.__call__(*[self, *args], **kwargs)  # type: ignore

    return cast(DecoratedFunctionType, wrapper)
