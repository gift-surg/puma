from time import monotonic
from typing import Any, List, Optional

from puma.buffer import Observable, Subscriber
from puma.runnable import MultiBufferServicingRunnable


class MonitorRunnable(MultiBufferServicingRunnable, Subscriber[Any]):
    """
    A Runnable to simplify monitoring one or more buffers whilst debugging / developing
    """

    def __init__(self, observables: List[Observable[Any]], name: str):
        MultiBufferServicingRunnable.__init__(self, name, [])
        for observable in observables:
            self._add_subscription(observable, self)

    def on_value(self, value: Any) -> None:
        print(monotonic(), value)

    def on_complete(self, error: Optional[BaseException]) -> None:
        print(monotonic(), f"Complete: {error if error else 'No error'}")
