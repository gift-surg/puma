from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Type

from puma.attribute import ProcessAction, ThreadAction, copied, manually_managed
from puma.attribute.attribute.sharing_attribute_between_scopes_not_allowed_error import SharingAttributeBetweenScopesNotAllowedError
from puma.attribute.mixin import ScopedAttributesMixin
from puma.environment import Environment
from puma.runnable import CommandDrivenRunnable
from puma.runnable.decorator.run_in_child_scope import run_in_child_scope
from tests.parameterized import NamedTestParameters


class ValidParams(NamedTestParameters):

    def __init__(self, environment: Environment, runnable: "TestValidRunnable", expected_context_count: int,
                 expected_context_count_remote: Optional[int] = None) -> None:
        super().__init__(runnable.__class__.__name__)
        self.environment = environment
        self.runnable = runnable
        self.expected_context_count_local = expected_context_count
        self.expected_context_count_remote = expected_context_count_remote or expected_context_count


class InvalidParams(NamedTestParameters):

    def __init__(self, environment: Environment, runnable: "TestInvalidRunnable", expected_error_type: Type[SharingAttributeBetweenScopesNotAllowedError]) -> None:
        super().__init__(runnable.__class__.__name__)
        self.environment = environment
        self.runnable = runnable
        self.expected_error_type = expected_error_type


@dataclass(frozen=True)
class RemoteCounterValues:
    count: int
    sub_object: int


class TestRunnable(CommandDrivenRunnable, ABC):
    count: int = copied("count")

    def __init__(self) -> None:
        super().__init__(self.__class__.__name__, [])


class TestValidRunnable(TestRunnable, ABC):
    sub_object: "TestRunnableSubObject" = copied("sub_object")

    def __init__(self) -> None:
        super().__init__()
        self.count = 500
        self.sub_object = self._create_sub_object()

    @abstractmethod
    def _create_sub_object(self) -> "TestRunnableSubObject":
        raise NotImplementedError()

    @run_in_child_scope
    def increment_value(self) -> None:
        if self.count is None:
            self.count = 100
        self.count += 1

        if self.sub_object.attr is None:
            self.sub_object.attr = 100
        self.sub_object.attr += 1

    @run_in_child_scope
    def get_remote_counter_value(self) -> RemoteCounterValues:
        return RemoteCounterValues(self.count, self.sub_object.attr)


class TestInvalidRunnable(TestRunnable):
    pass


class ThreadSharedTestRunnable(TestValidRunnable):
    count: int = manually_managed("count", ThreadAction.SHARED, ProcessAction.NOT_ALLOWED)

    def _create_sub_object(self) -> "TestRunnableSubObject":
        return ThreadSharedSubObject()


class ThreadCopiedTestRunnable(TestValidRunnable):
    count: int = manually_managed("count", ThreadAction.COPIED, ProcessAction.NOT_ALLOWED)

    def _create_sub_object(self) -> "TestRunnableSubObject":
        return ThreadCopiedSubObject()


class ThreadNotAllowedTestRunnable(TestInvalidRunnable):
    count: int = manually_managed("count", ThreadAction.NOT_ALLOWED, ProcessAction.NOT_ALLOWED)


class ProcessCopiedTestRunnable(TestValidRunnable):
    count: int = manually_managed("count", ThreadAction.NOT_ALLOWED, ProcessAction.COPIED)

    def _create_sub_object(self) -> "TestRunnableSubObject":
        return ProcessCopiedSubObject()


class ProcessSetToNoneTestRunnable(TestValidRunnable):
    count: int = manually_managed("count", ThreadAction.NOT_ALLOWED, ProcessAction.SET_TO_NONE)

    def _create_sub_object(self) -> "TestRunnableSubObject":
        return ProcessSetToNoneSubObject()


class ProcessNotAllowedTestRunnable(TestInvalidRunnable):
    count: int = manually_managed("count", ThreadAction.NOT_ALLOWED, ProcessAction.NOT_ALLOWED)


class TestRunnableSubObject(ScopedAttributesMixin):
    attr: int = copied("attr")

    def __init__(self) -> None:
        super().__init__()
        self.attr = 500


class ThreadSharedSubObject(TestRunnableSubObject):
    attr: int = manually_managed("attr", ThreadAction.SHARED, ProcessAction.NOT_ALLOWED)


class ThreadCopiedSubObject(TestRunnableSubObject):
    attr: int = manually_managed("attr", ThreadAction.COPIED, ProcessAction.NOT_ALLOWED)


class ProcessCopiedSubObject(TestRunnableSubObject):
    attr: int = manually_managed("attr", ThreadAction.NOT_ALLOWED, ProcessAction.COPIED)


class ProcessSetToNoneSubObject(TestRunnableSubObject):
    attr: int = manually_managed("attr", ThreadAction.NOT_ALLOWED, ProcessAction.SET_TO_NONE)
