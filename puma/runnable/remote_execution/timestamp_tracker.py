from collections import deque
from time import monotonic
from typing import Deque, List, Optional, cast

MIN_TIMESTAMP = 1e-100

TimestampDeque = Deque[float]


class TimestampTracker:
    """A deque for storing Timestamps, proving a mechanism for retrieving timestamps that were recorded during a given time window"""

    def __init__(self, size: int = 10) -> None:
        self._index = 0
        self._size = size
        self._arr: TimestampDeque = deque(maxlen=size)

    def record(self, timestamp: float) -> None:
        """Inserts a timestamp into the buffer"""
        self._arr.append(timestamp)
        self._index += 1

    def entries_in_last_n_seconds(self, look_back_seconds: float,
                                  now_timestamp: Optional[float] = None) -> List[float]:
        """Retrieves stored timestamps that meet the given look back criteria"""
        if not now_timestamp:
            now_timestamp = monotonic()

        compare_timestamp = max(MIN_TIMESTAMP, now_timestamp - look_back_seconds)

        recent_timestamps: TimestampDeque = deque(maxlen=self._size)
        for timestamp in self._arr:
            if timestamp >= compare_timestamp:
                # TODO: Improve this, as once a timestamp is greater, all subsequent ones will be too
                recent_timestamps.append(timestamp)

        return cast(List[float], recent_timestamps)
