from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, List
from unittest import TestCase

from puma.attribute import copied
from puma.attribute.mixin import ScopedAttributesMixin
from puma.buffer import Publishable
from puma.helpers.os import is_windows
from puma.scope_id import get_current_scope_id


class AllMethodsReturnNone(ABC):

    @abstractmethod
    def no_args(self) -> None:
        raise NotImplementedError()

    @abstractmethod
    def one_arg(self, a: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def two_args(self, a: str, b: str) -> None:
        raise NotImplementedError()


class HasMethodThatReturnsValue(AllMethodsReturnNone, ABC):

    @abstractmethod
    def returns_value(self, a: str, b: int) -> str:
        raise NotImplementedError()


class ComplexObject:

    def __init__(self, str_value: str, number: int, boolean: bool):
        self._str_value = str_value
        self._number = number
        self._boolean = boolean

    def get_str_value(self) -> str:
        return self._str_value

    def get_number(self) -> int:
        return self._number

    def get_bool(self) -> bool:
        return self._boolean


class HasMethodThatReturnsComplexObject(HasMethodThatReturnsValue, ABC):

    @abstractmethod
    def get_complex_object(self, str_value: str, number: int, boolean: bool) -> ComplexObject:
        raise NotImplementedError()


@dataclass(frozen=True)
class CallResponse:
    method_name: str
    args: List[Any] = field(default_factory=list)
    scope_id: str = field(init=False, default_factory=get_current_scope_id)


class SendsCallsToBufferImpl(HasMethodThatReturnsValue, ScopedAttributesMixin):
    _feedback_publishable: Publishable[CallResponse] = copied("_feedback_publishable")

    def __init__(self, feedback_publishable: Publishable[CallResponse]) -> None:
        super().__init__()
        self._feedback_publishable = feedback_publishable

    def no_args(self) -> None:
        with self._feedback_publishable.publish() as publisher:
            publisher.publish_value(CallResponse("no_args"))

    def one_arg(self, a: str) -> None:
        with self._feedback_publishable.publish() as publisher:
            publisher.publish_value(CallResponse("one_arg", [a]))

    def two_args(self, a: str, b: str) -> None:
        with self._feedback_publishable.publish() as publisher:
            publisher.publish_value(CallResponse("two_args", [a, b]))

    def returns_value(self, a: str, b: int) -> str:
        with self._feedback_publishable.publish() as publisher:
            publisher.publish_value(CallResponse("returns_value", [a, b]))
        return f"Value returned by {self.__class__.__name__}"


class SubObject:

    @abstractmethod
    def set_attribute(self, new_value: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def get_attribute(self) -> str:
        raise NotImplementedError()


class SubObjectImpl(SubObject):

    def __init__(self) -> None:
        self._attribute = "Initial State"

    def set_attribute(self, new_value: str) -> None:
        self._attribute = new_value

    def get_attribute(self) -> str:
        return self._attribute


class Parent:

    @abstractmethod
    def get_sub_object(self) -> SubObject:
        raise NotImplementedError()


class ParentImpl(Parent):

    def get_sub_object(self) -> SubObject:
        return SubObjectImpl()


@dataclass(frozen=True)
class Scope:
    pid: str
    ident: str


def from_scope_id(scope_id: str) -> Scope:
    elements = scope_id.split("-")
    return Scope(elements[0], elements[1])


class EnvironmentVerifier:

    def verify(self, test_case: TestCase, scope_id_a: Scope, scope_id_b: Scope) -> None:
        raise NotImplementedError()


class DifferentProcessVerifier(EnvironmentVerifier):

    def verify(self, test_case: TestCase, scope_id_a: Scope, scope_id_b: Scope) -> None:
        if is_windows():
            # On Windows, expect both PID and ident to be different
            test_case.assertNotEqual(scope_id_a.pid, scope_id_b.pid)
            test_case.assertNotEqual(scope_id_a.ident, scope_id_b.ident)
        else:
            # On Linux, PID is different but ident may be the same or different
            test_case.assertNotEqual(scope_id_a.pid, scope_id_b.pid)


class DifferentThreadVerifier(EnvironmentVerifier):

    def verify(self, test_case: TestCase, scope_id_a: Scope, scope_id_b: Scope) -> None:
        test_case.assertEqual(scope_id_a.pid, scope_id_b.pid)
        test_case.assertNotEqual(scope_id_a.ident, scope_id_b.ident)


DIFFERENT_PROCESS = DifferentProcessVerifier()
DIFFERENT_THREAD = DifferentThreadVerifier()
