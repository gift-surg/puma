from puma.mixin import Mixin


class NotATestCase(Mixin):
    """Mixin to prevent a class with 'Test' in the name being detected as a TestCase"""
    __test__ = False
