from tests.mixin import NotATestCase


class TestVal(NotATestCase):

    def __init__(self, counter: int, timestamp: float, *, large: bool = False) -> None:
        super().__init__()
        self.counter = counter
        self.timestamp = timestamp
        self._data_load = "a" * 1000000 if large else None

    def __str__(self) -> str:
        return f"TestVal(counter={self.counter}, timestamp={self.timestamp}, large={self._data_load is not None})"
