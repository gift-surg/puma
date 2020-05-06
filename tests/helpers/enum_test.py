from enum import Enum, auto, unique
from typing import Any, Type
from unittest import TestCase

from puma.helpers.enum import EnumMemberError, ensure_enum_member
from puma.helpers.testing.mixin import NotATestCaseEnum
from puma.helpers.testing.parameterized import NamedTestParameters, parameterized


class TestEnum(NotATestCaseEnum):
    MEMBER0 = 0
    MEMBER1 = 1
    MEMBER2 = 1
    MEMBER3 = 3
    MEMBER4 = 4


@unique
class UniqueTestEnum(NotATestCaseEnum):
    MEMBER0 = 0
    MEMBER1 = 1
    MEMBER2 = 2
    MEMBER3 = 3
    MEMBER4 = 4


@unique
class UniqueAutoTestEnum(NotATestCaseEnum):
    MEMBER0 = auto()
    MEMBER1 = auto()
    MEMBER2 = auto()
    MEMBER3 = auto()
    MEMBER4 = auto()


class AutoTestEnum(NotATestCaseEnum):
    MEMBER0 = auto()
    MEMBER1 = auto()
    MEMBER2 = auto()
    MEMBER3 = auto()
    MEMBER4 = auto()


class EnumTestParameters(NamedTestParameters):

    def __init__(self, enum_factory: Type[Enum]) -> None:
        super().__init__(enum_factory.__name__)
        self._enum_factory = enum_factory

    @property
    def enum_class(self) -> Type[Enum]:
        return self._enum_factory


enum_classes = [
    EnumTestParameters(TestEnum),
    EnumTestParameters(UniqueTestEnum),
    EnumTestParameters(UniqueAutoTestEnum),
    EnumTestParameters(AutoTestEnum),
]


class EnumHelperTest(TestCase):

    @parameterized(enum_classes)
    def test_enum_members_recognised(self, env: EnumTestParameters) -> None:
        member: Any
        for member in list(env.enum_class):
            ensure_enum_member(member, env.enum_class)

    @parameterized(enum_classes)
    def test_non_members_raise(self, env: EnumTestParameters) -> None:
        for non_member in range(-1, -10, -1):
            with self.assertRaises(EnumMemberError):
                ensure_enum_member(non_member, env.enum_class)
        for non_member in range(5, 10):
            with self.assertRaises(EnumMemberError):
                ensure_enum_member(non_member, env.enum_class)

    @parameterized(enum_classes)
    def test_none_raises(self, env: EnumTestParameters) -> None:
        with self.assertRaises(EnumMemberError):
            ensure_enum_member(None, env.enum_class)

    @parameterized(enum_classes)
    def test_bool_values_raise(self, env: EnumTestParameters) -> None:
        for non_member in [True, False]:
            with self.assertRaises(EnumMemberError):
                ensure_enum_member(non_member, env.enum_class)

    @parameterized(enum_classes)
    def test_irrelevant_values_raise(self, env: EnumTestParameters) -> None:
        for non_member in ['member1', -1, object(), TypeError()]:
            with self.assertRaises(EnumMemberError):
                ensure_enum_member(non_member, env.enum_class)
