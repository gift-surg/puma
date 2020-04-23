import pickle
from abc import ABC
from typing import Any, Generic, List

from puma.buffer import Publishable
from puma.buffer.implementation.managed_queues import ManagedQueueTypes
from puma.context import ContextManager, Exit_1, Exit_2, Exit_3, ensure_used_within_context_manager, must_be_context_managed
from puma.environment import Environment, ProcessEnvironment, ThreadEnvironment
from puma.runnable.pool import PoolJob, PoolJobCall, PoolRunnable, PoolType
from puma.runnable.runner import Runner
from puma.timeouts import TIMEOUT_INFINITE, Timeouts


@must_be_context_managed
class Pool(Generic[PoolType], ContextManager["Pool[PoolType]"], ABC):

    def __init__(self, size: int, name: str, result_buffer: Publishable[PoolType], environment: Environment) -> None:
        self._size = size
        self._name = name
        self._result_buffer: Publishable[PoolType] = result_buffer
        self._environment = environment
        self._pool_job_queue: ManagedQueueTypes[PoolJobCall] = self._environment.create_managed_queue(PoolJobCall, 1)
        self._pool_runnables: List[PoolRunnable] = []
        self._pool_runners: List[Runner] = []

    def __enter__(self) -> "Pool[PoolType]":

        self._pool_job_queue.__enter__()
        self._result_publisher = self._result_buffer.publish().__enter__()

        for i in range(self._size):
            name_suffix = f"#{i} in {self._name}"
            runnable = self._create_pool_runnable(f"Runnable {name_suffix}", self._pool_job_queue)
            runner = self._environment.create_runner(runnable, f"Runner {name_suffix}")

            runner.__enter__()
            runner.start()

            self._pool_runnables.append(runnable)
            self._pool_runners.append(runner)

        for runner in self._pool_runners:
            runner.wait_until_running()

        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        # Kill all runners
        for runner in self._pool_runners:
            runner.stop()

        for runner in self._pool_runners:
            runner.join()

        for runner in self._pool_runners:
            runner.__exit__(exc_type, exc_value, traceback)

        self._result_publisher.publish_complete(None)

        self._result_publisher.__exit__(exc_type, exc_value, traceback)

        self._pool_runnables = []
        self._pool_runners = []

    @ensure_used_within_context_manager
    def submit(self, job: PoolJob, args: Any, timeout: float = TIMEOUT_INFINITE) -> None:

        # Ensure job can be pickled
        try:
            pickle.dumps(job)
        except AttributeError as e:
            raise RuntimeError("Please provide a pickleable job (ie, not a lambda)") from e

        # Ensure args can be pickled
        try:
            pickle.dumps(args)
        except AttributeError:
            for arg in args:
                try:
                    pickle.dumps(arg)
                except AttributeError as e:
                    raise RuntimeError(f"Please provide pickleable arguments, unable to pickle {arg}") from e

        # Check for any exceptions that have occurred in the runners
        for runner in self._pool_runners:
            runner.check_for_exceptions()

        self._pool_job_queue.put(PoolJobCall(job, args), True, Timeouts.timeout_for_queue(timeout))

    def _create_pool_runnable(self, name: str, queue: ManagedQueueTypes[PoolJobCall]) -> PoolRunnable:
        return PoolRunnable(name, self._result_buffer, queue)


class ThreadPool(Pool):

    def __init__(self, size: int, name: str, result_buffer: Publishable[PoolType]) -> None:
        super().__init__(size, name, result_buffer, ThreadEnvironment())


class ProcessPool(Pool):

    def __init__(self, size: int, name: str, result_buffer: Publishable[PoolType]) -> None:
        super().__init__(size, name, result_buffer, ProcessEnvironment())
