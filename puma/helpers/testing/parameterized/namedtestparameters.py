from abc import ABC
from typing import Iterator


class NamedTestParameters(ABC):
    """
    Creates a named set of parameterized test parameters to be used with @parameterized() without having to create a dummy "name" test argument
    """

    def __init__(self, name: str) -> None:
        self._name = name

    def __iter__(self) -> Iterator["NamedTestParameters"]:
        """
        Must be a list with a single entry of itself so that it works with the "parameterized" library
        """
        return [self].__iter__()

    @property
    def name(self) -> str:
        return self._name
