from enum import auto, unique

from tests.mixin.not_a_test_case_enum import NotATestCaseEnum


@unique
class TestExecutionMode(NotATestCaseEnum):
    WaitUntilStopped = auto()
    EndQuicklyWithoutError = auto()
    EndQuicklyWithError = auto()
    IgnoreStopCommand = auto()
