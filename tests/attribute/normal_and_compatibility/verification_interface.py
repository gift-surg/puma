from abc import ABC, abstractmethod
from typing import Generic, Optional, Type, TypeVar

from tests.attribute.normal_and_compatibility.scoped_attributes_test_case import ScopedAttributesTestCase

C = TypeVar("C")


class VerificationInterface(Generic[C], ABC):

    @abstractmethod
    def get_test_class(self) -> Type[C]:
        raise NotImplementedError()

    @abstractmethod
    def local_verification_method(self, test_case: ScopedAttributesTestCase, obj: C) -> None:
        raise NotImplementedError()

    def get_process_start_expected_error_message(self) -> Optional[str]:
        return None

    @abstractmethod
    def remote_verification_method(self, test_case: ScopedAttributesTestCase, obj: C) -> None:
        raise NotImplementedError()
