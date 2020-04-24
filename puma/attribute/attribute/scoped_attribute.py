from abc import ABC, abstractmethod
from typing import Any, Generic, Type, TypeVar

from puma.attribute import AccessibleScope, ProcessAction, ThreadAction

SCOPE_ID_SHARED = "SHARED"
T = TypeVar("T")


class ScopedAttribute(Generic[T], ABC):
    """A Data Descriptor (https://docs.python.org/3.7/howto/descriptor.html) that represents a class attribute that should have its scope automatically managed"""

    @abstractmethod
    def __get__(self, instance: Any, owner: Type[Any]) -> T:
        """Retrieve the attribute value"""
        raise NotImplementedError()

    @abstractmethod
    def __set__(self, instance: Any, value: T) -> None:
        """Set the attribute value"""
        raise NotImplementedError()

    @property
    @abstractmethod
    def name(self) -> str:
        """Get the attribute name"""
        raise NotImplementedError()

    @property
    @abstractmethod
    def accessible_scope(self) -> AccessibleScope:
        """Get the scopes in which this attribute may be accessed (parent, child or both)"""
        raise NotImplementedError()

    @abstractmethod
    def assert_can_be_copied_to_another_process(self) -> None:
        """Checks that the attribute can be copied to another process"""
        raise NotImplementedError()

    @abstractmethod
    def get_attribute_id(self, attribute_name: str) -> str:
        """Get the attribute id - used to store and retrieve the attribute's value from a particular object instance"""
        raise NotImplementedError()

    @property
    @abstractmethod
    def thread_action(self) -> ThreadAction:
        """Get the ThreadAction for this attribute"""
        raise NotImplementedError()

    @property
    @abstractmethod
    def process_action(self) -> ProcessAction:
        """Get the ProcessAction for this attribute"""
        raise NotImplementedError()
