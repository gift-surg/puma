import pickle
from abc import ABC, abstractmethod
from typing import Any, Generic, Optional, TypeVar, Union

from puma.attribute.attribute.sharing_attribute_between_scopes_not_allowed_error import SharingAttributeBetweenScopesNotAllowedError

T = TypeVar("T")


def can_be_shared_without_copying(obj: Any) -> bool:
    from puma.attribute.mixin import ScopedAttributesBaseMixin
    return isinstance(obj, ScopedAttributesBaseMixin)


def assert_is_sharable(obj: Any) -> None:
    if can_be_shared_without_copying(obj):
        pass  # Do nothing, as the item can be shared
    else:
        # Ensure that it can be copied successfully
        try:
            pickle.dumps(obj)
        except SharingAttributeBetweenScopesNotAllowedError:
            # Ignore attributes that are NOT_ALLOWED - these will fail with useful messages once the Thread or Process attempts to launch
            pass
        except RuntimeError as re:
            # Handle known "errors"

            # 1. This error is raised as the objects (multiprocessing.Queue etc.) cannot be pickled outside of launching a process
            # 2. This error is raised if a method of a class that contains a MultiThreadBuffer is given (typically a method belonging to a Runnable).
            #       MultiThreadBuffers cannot be pickled, but 99% of the time the class will never be pickled as it will be run in a different Thread (not Process).
            #       In the other 1% of cases an error will be thrown when attempting to launch the process. This check is designed to give an early error which clearly indicates
            #       where the issue is; ie when an unpickleable method was given to a ScopedAttribute "factory" method
            if str(re).endswith("objects should only be shared between processes through inheritance") or \
                    str(re) == "MultiThreadBuffers cannot be shared across processes - use a MultiProcessBuffer instead":
                # ignore
                pass
            else:
                raise re
        except Exception as e:
            raise AssertionError(f"{obj} is not shareable") from e


class AttributeProxy(Generic[T], ABC):
    """A proxy for accessing InstanceWrapper attributes"""

    @abstractmethod
    def get(self, default_value_or_accessor: Optional[Union[T, "ValueNotFoundHandler[T]"]] = None) -> T:
        """
        Gets the attribute value, falling back to a given default value or accessor

        :param default_value_or_accessor Either a default value or a ValueNotFoundHandler that returns the desired default value
        """
        raise NotImplementedError()

    @abstractmethod
    def set(self, value: T) -> None:
        """Sets the attribute value"""
        raise NotImplementedError()

    @abstractmethod
    def storage_key(self, scope_id: Optional[str] = None) -> str:
        raise NotImplementedError()

    @property
    @abstractmethod
    def fully_qualified_name(self) -> str:
        """Returns the attribute name"""
        raise NotImplementedError()

    @property
    @abstractmethod
    def parent_attribute_name(self) -> str:
        """Returns the attribute short name"""
        raise NotImplementedError()

    @property
    @abstractmethod
    def instance(self) -> Any:
        """Returns the underlying object that is storing the attribute values"""
        raise NotImplementedError()


VT = TypeVar("VT")


class ValueNotFoundHandler(Generic[VT], ABC):
    """ An interface for retrieving values for attributes that have not yet been defined in the current scope"""

    @abstractmethod
    def get_value(self, attribute_proxy_instance: AttributeProxy) -> VT:
        """ Method that attempts to retrieve a value"""
        raise NotImplementedError()
