from logging import LogRecord
from typing import Union

from puma.buffer.implementation.managed_queues import ManagedProcessQueue


class ManagedProcessLogQueue(ManagedProcessQueue[LogRecord]):
    """Derived ManagedProcessQueue specifically for logging.

    The difference from the base class is:
      - If put() is called outside of context management, this is silently ignored, rather than causing an error as it would normally.
        This preventing a live-lock situation where an error is constantly logged, which prevents the queue being cleaned up.
    """

    def put(self, obj: LogRecord, block: bool = True, timeout: Union[int, float, None] = None) -> None:
        if not self._in_context_management:
            # Silently discard  - don't log here!
            return
        super().put(obj, block, timeout)
