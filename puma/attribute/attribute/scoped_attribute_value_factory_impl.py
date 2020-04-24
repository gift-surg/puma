from typing import Generic, NoReturn, Optional, TypeVar

from puma.attribute import ValueFactory, unmanaged
from puma.attribute.attribute.detected_scope import DetectedScope
from puma.attribute.attribute.scoped_attribute_value_factory import ScopedAttributeValueFactory
from puma.attribute.attribute.sharing_attribute_between_scopes_not_allowed_error import SharingAttributeBetweenScopesNotAllowedError
from puma.attribute.attribute_proxy.attribute_proxy import assert_is_sharable

T = TypeVar("T")


class ScopedAttributeValueFactoryImpl(Generic[T], ScopedAttributeValueFactory[T]):
    _value_factory: ValueFactory[T] = unmanaged("_value_factory")

    def __init__(self, value_factory: ValueFactory[T]) -> None:
        super().__init__()
        # Ensure value factory is pickleable
        try:
            assert_is_sharable(value_factory)
        except AssertionError as ae:
            self._raise_unpickleable_error(ae)
        except SharingAttributeBetweenScopesNotAllowedError as e:
            self._raise_unpickleable_error(e, "your ValueFactory appears to be part of a class that has an attribute which is not allowed to be shared")
        except Exception as e:
            if str(e).lower().startswith("can't pickle"):
                self._raise_unpickleable_error(e)
            else:
                raise e
        # MyPy complains about assigning to a method: https://github.com/python/mypy/issues/708
        self._value_factory: ValueFactory[T] = value_factory  # type: ignore

    def _raise_unpickleable_error(self, error: Exception, additional_message: Optional[str] = None) -> NoReturn:
        additional_message = f": {additional_message}" if additional_message else ""
        raise TypeError(f"ValueFactory must be pickleable{additional_message}") from error

    def get_value(self, detected_scope: DetectedScope) -> T:
        # MyPy again complains erroneously: https://github.com/python/mypy/issues/708
        return self._value_factory()  # type: ignore
