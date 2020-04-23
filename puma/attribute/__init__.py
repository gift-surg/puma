import logging
import re
from typing import Callable, TypeVar, cast

from puma.attribute.accessible_scope import AccessibleScope
from puma.attribute.attribute.thread_and_process_actions import ProcessAction, ThreadAction

logger = logging.getLogger(__name__)

T = TypeVar("T")
VF_T = TypeVar("VF_T")

ValueFactory = Callable[[], VF_T]

ATTRIBUTE_NAME_PREFIX = "_SMA"
ATTRIBUTE_NAME_SEPARATOR = "~"

SCOPE_ID_DELIMITER = "#"
SCOPE_ID_PLACEHOLDER = f"{SCOPE_ID_DELIMITER}@{SCOPE_ID_DELIMITER}"
SCOPE_ID_REGEX = re.compile(f"{SCOPE_ID_DELIMITER}.*{SCOPE_ID_DELIMITER}")


def child_only(name: str) -> T:
    """
    Return an ScopedAttribute that is only accessible from the child scope

    :param name The attribute's name. This should match the name given to the attribute; ie
        class A:
            attr: int = child_only("attr")
    """
    from puma.attribute.attribute.scoped_attribute_impl import ScopedAttributeImpl
    return cast(T, ScopedAttributeImpl(name, AccessibleScope.child, ThreadAction.COPIED, ProcessAction.COPIED))


def parent_only(name: str) -> T:
    """
    Return an ScopedAttribute that is only accessible from the parent scope

    :param name The attribute's name. This should match the name given to the attribute; ie
        class A:
            attr: int = parent_only("attr")
    """
    from puma.attribute.attribute.scoped_attribute_impl import ScopedAttributeImpl
    return cast(T, ScopedAttributeImpl(name, AccessibleScope.parent, ThreadAction.NOT_ALLOWED, ProcessAction.NOT_ALLOWED))


def copied(name: str) -> T:
    """
    Return an ScopedAttribute that is accessible from both the parent and child scope

    :param name The attribute's name. This should match the name given to the attribute; ie
        class A:
            attr: int = copied("attr")
    """
    from puma.attribute.attribute.scoped_attribute_impl import ScopedAttributeImpl
    return cast(T, ScopedAttributeImpl(name, AccessibleScope.shared, ThreadAction.COPIED, ProcessAction.COPIED))


def unmanaged(name: str) -> T:
    """
        Return an ScopedAttribute whose value is both accessible from both the parent and child scope and behaves according to Python's defaults.
        That is:
            - Using Processes there will be an independent copy of the object in each scope
            - Using Threads there will be a shared instance of the object available from both scopes

        ## WARNING ##
        This type is not recommended unless it is specifically required or you know what you are doing!

        :param name The attribute's name. This should match the name given to the attribute; ie
            class A:
                attr: int = unmanaged("attr")
        """
    from puma.attribute.attribute.unmanaged_scoped_attribute import UnmanagedScopedAttribute
    return cast(T, UnmanagedScopedAttribute(name))


def python_default(name: str) -> T:
    """
    A shortcut for "manually_managed(name, ThreadAction.shared, ProcessAction.copied)"

    This is how Python behaves by default but, in comparison to using "unmanaged", factory methods (such as per_scope_value) can be used

    :param name The attribute's name. This should match the name given to the attribute; ie
        class A:
            attr: int = python_default("attr")
    """
    return manually_managed(name, ThreadAction.SHARED, ProcessAction.COPIED)


def manually_managed(name: str, thread_action: ThreadAction, process_action: ProcessAction) -> T:
    """
        Return an ScopedAttribute whose value is both accessible from both the parent and child scope and behaves according to given Thread and ProcessAction.
        That is:
            ThreadAction
                - copied - value will be copied (or regenerated according to factory methods) between Threads
                - shared - value will be shared between Threads
                - not_allowed - attribute cannot be passed to child Threads. Attempting to do so will raise an error
            ProcessAction
                - copied - value will be copied (or regenerated according to factory methods) between Processes
                - not_allowed - attribute cannot be passed to child Processes. Attempting to do so will raise an error

        :param name The attribute's name. This should match the name given to the attribute; ie
        :param thread_action The action to take when passing the attribute to a child Thread
        :param process_action The action to take when passing the attribute to a child Process
            class A:
                attr: int = manually_managed("attr", ThreadAction.copied, ProcessAction.copied)
        """
    from puma.attribute.attribute.scoped_attribute_impl import ScopedAttributeImpl
    return cast(T, ScopedAttributeImpl(name, AccessibleScope.shared, thread_action, process_action))


def child_scope_value(value: T) -> T:
    """
    Return an ScopedAttributeValueFactory that sets the attribute to the given value at the beginning of the child scope

    :param value: The value to set the attribute to at the beginning of the child scope
    """
    return per_scope_value(value)


def per_scope_value(value: T) -> T:
    """
    Return an ScopedAttributeValueFactory that sets the attribute to the given value at the beginning of each scope
    The value given must be pickleable or an error will be raised. For unpickleable values use scope_specific and provide a factory for the child scope

    :param value: The value to set the attribute to at the beginning of each scope
    """
    from puma.attribute.attribute.per_scope_value_scoped_attribute_value_factory import PerScopeValueScopedAttributeValueFactory
    return cast(T, PerScopeValueScopedAttributeValueFactory(value))


def factory(value_factory: ValueFactory[T]) -> T:
    """
    Return an ScopedAttributeValueFactory that sets the attribute to a new instance of the result of the given factory at the beginning of each scope

    :param value_factory: A function that returns a new instance of the desired value to set the attribute to at the beginning of each scope
    """
    from puma.attribute.attribute.scoped_attribute_value_factory_impl import ScopedAttributeValueFactoryImpl
    return cast(T, ScopedAttributeValueFactoryImpl(value_factory))


def scope_specific(parent_value: T, child_value_factory: ValueFactory[T]) -> T:
    """
    Return an ScopedAttributeValueFactory that sets the attribute to the specified values depending on the current scope

    This method is similar to per_scope_value except that it handles non-pickleable values by allowing a factory to be provided for creating the value in the child scope

    :param parent_value: The value to set the attribute to at the beginning of the parent scope
    :param child_value_factory: A function that returns a new instance of the desired value to set the attribute to at the beginning of the child scope
    """
    from puma.attribute.attribute.scope_specific_scoped_attribute_value_factory import ScopeSpecificScopedAttributeValueFactory
    return cast(T, ScopeSpecificScopedAttributeValueFactory(parent_value, child_value_factory))
