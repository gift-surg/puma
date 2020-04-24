from collections.abc import Iterable, Sized
from typing import Dict, Iterator, List, Set, Union, no_type_check
from unittest import TestCase

from tests.test_case_helpers import assertEmpty, assertNotEmpty, is_empty, is_not_empty


class SizedButNotIterable(Sized):
    def __init__(self, length: int) -> None:
        self._length = length

    def __len__(self) -> int:
        return self._length


class IterableButNotSized(Iterable):
    def __init__(self, length: int) -> None:
        self._stuff = 'a' * length

    def __iter__(self) -> Iterator[str]:
        return iter(self._stuff)


EMPTY_LIST: List[str] = list()
EMPTY_DICT: Dict[str, str] = dict()
EMPTY_SET: Set[str] = set()
EMPTY_STRING = ''
EMPTY_SIZED_THING = SizedButNotIterable(0)
EMPTY_ITERABLE_THING = IterableButNotSized(0)

POPULATED_LIST = ['a']
POPULATED_DICT = {'a': 'a'}
POPULATED_SET = {'a'}
POPULATED_STRING = 'a'
POPULATED_SIZED_THING = SizedButNotIterable(1)
POPULATED_ITERABLE_THING = IterableButNotSized(1)

EMPTY_CONTAINERS: List[Union[Sized, Iterable]] = [EMPTY_LIST, EMPTY_DICT, EMPTY_SET, EMPTY_STRING, EMPTY_SIZED_THING, EMPTY_ITERABLE_THING]
POPULATED_CONTAINERS: List[Union[Sized, Iterable]] = [POPULATED_LIST, POPULATED_DICT, POPULATED_SET, POPULATED_STRING, POPULATED_SIZED_THING, POPULATED_ITERABLE_THING]


class TestCaseHelpersTest(TestCase):
    def test_is_empty_true(self) -> None:
        for empty_thing in EMPTY_CONTAINERS:
            self.assertTrue(is_empty(empty_thing))

    def test_is_empty_false(self) -> None:
        for populated_thing in POPULATED_CONTAINERS:
            self.assertFalse(is_empty(populated_thing))

    def test_is_not_empty_true(self) -> None:
        for populated_thing in POPULATED_CONTAINERS:
            self.assertTrue(is_not_empty(populated_thing))

    def test_is_not_empty_false(self) -> None:
        for empty_thing in EMPTY_CONTAINERS:
            self.assertFalse(is_not_empty(empty_thing))

    def test_assert_empty_when_empty(self) -> None:
        for empty_thing in EMPTY_CONTAINERS:
            assertEmpty(self, empty_thing)  # no assertion

    def test_assert_empty_when_not_empty_with_message(self) -> None:
        for populated_thing in POPULATED_CONTAINERS:
            with self.assertRaisesRegex(AssertionError, "My message"):
                assertEmpty(self, populated_thing, "My message")

    def test_assert_empty_when_not_empty_without_message(self) -> None:
        for populated_thing in POPULATED_CONTAINERS:
            with self.assertRaises(AssertionError):
                assertEmpty(self, populated_thing)

    def test_assert_not_empty_when_empty_with_message(self) -> None:
        for empty_thing in EMPTY_CONTAINERS:
            with self.assertRaisesRegex(AssertionError, "My message"):
                assertNotEmpty(self, empty_thing, "My message")

    def test_assert_not_empty_when_empty_without_message(self) -> None:
        for empty_thing in EMPTY_CONTAINERS:
            with self.assertRaises(AssertionError):
                assertNotEmpty(self, empty_thing)

    def test_assert_not_empty_when_not_empty(self) -> None:
        for populated_thing in POPULATED_CONTAINERS:
            assertNotEmpty(self, populated_thing)  # no assertion

    # noinspection PyTypeChecker
    @no_type_check
    def test_illegal_params(self) -> None:
        with self.assertRaises(TypeError):
            is_empty(None)
        with self.assertRaises(TypeError):
            is_empty(1)
        with self.assertRaises(TypeError):
            is_not_empty(None)
        with self.assertRaises(TypeError):
            is_not_empty(1)
        with self.assertRaises(TypeError):
            assertEmpty(None, EMPTY_LIST)
        with self.assertRaises(TypeError):
            assertEmpty(self, None)
        with self.assertRaises(TypeError):
            assertEmpty(self, 1)
        with self.assertRaises(TypeError):
            assertNotEmpty(None, EMPTY_LIST)
        with self.assertRaises(TypeError):
            assertNotEmpty(self, None)
        with self.assertRaises(TypeError):
            assertNotEmpty(self, 1)
