from enum import Enum


class NotATestCaseEnum(Enum):
    """Base Enum to prevent a class with 'Test' in the name being detected as a TestCase"""
    __test__ = False
