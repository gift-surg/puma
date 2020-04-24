import queue
from typing import Any, Callable, Generic

from puma.attribute import child_only, copied
from puma.buffer import Publishable, Publisher
from puma.buffer.implementation.managed_queues import ManagedQueueTypes
from puma.runnable import CommandDrivenRunnable
from puma.runnable.pool import PoolJobCall, PoolType


class PoolRunnable(Generic[PoolType], CommandDrivenRunnable):
    _result_publisher: Publisher[PoolType] = child_only("_result_publisher")
    _job_queue: ManagedQueueTypes[PoolJobCall] = copied("_job_queue")

    def __init__(self, name: str, result_publishable: Publishable[PoolType], job_queue: ManagedQueueTypes[PoolJobCall]):
        super().__init__(name, [result_publishable])
        self._result_publisher: Publisher[PoolType] = self._get_publisher(result_publishable)
        self._job_queue = job_queue

    def _pre_wait_hook(self) -> None:
        # Retrieve any jobs from the queue
        try:
            job_call = self._job_queue.get(True, 0.1)
            self._result_publisher.publish_value(self._handle_job(job_call.job, *job_call.args))
        except queue.Empty:
            pass

        self._event.set()  # TODO: QueueHandlingRunnable??

    def _handle_job(self, job: Callable, *args: Any) -> Any:
        return job(*args)
