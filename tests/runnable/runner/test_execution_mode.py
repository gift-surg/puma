from enum import auto, unique

from puma.helpers.testing.mixin import NotATestCaseEnum


@unique
class TestExecutionMode(NotATestCaseEnum):
    WaitUntilStopped = auto()
    EndQuicklyWithoutError = auto()
    EndQuicklyWithError = auto()
    IgnoreStopCommand = auto()
