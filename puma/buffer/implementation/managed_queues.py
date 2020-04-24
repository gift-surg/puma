import queue
import typing
from abc import abstractmethod
from typing import Any, Dict, Optional, TypeVar, Union, no_type_check

from puma.buffer._queues import _ProcessQueue, _ThreadQueue
from puma.context import Exit_1, Exit_2, Exit_3
from puma.helpers.string import safe_str

T = TypeVar('T')

# It takes time for the multiprocessing queue to transfer items across - especially large items - so we need to allow a timeout when popping items for discarding them.
PROCESS_DISCARD_TIMEOUT = 0.1


class _ManagedQueueMixin(typing.Generic[T]):
    # Private mixin class for use only with ManagedThreadQueue and ManagedProcessQueue. Provides:
    #  - _name property
    #  - checks that put() is only used inside context management

    def __init__(self, name: Optional[str] = None, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)  # type: ignore   # Forwards all unused arguments; type ignore because of known issue https://github.com/python/mypy/issues/5887
        self._name = name
        self._in_context_management = False

    def __enter__(self) -> '_ManagedQueueMixin[T]':
        self._in_context_management = True
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        assert self._in_context_management
        self._in_context_management = False
        self.discard_queued_items()

    def put(self, obj: T, block: bool = True, timeout: Union[int, float, None] = None) -> None:
        if not self._in_context_management:
            if self._name:
                raise RuntimeError(f"ManagedQueue '{self._name}' being pushed to, outside of context management")
            else:
                raise RuntimeError("ManagedQueue being pushed to, outside of context management")
        try:
            # Call next class in MRO; type ignore because this class is not yet known,
            # see https://stackoverflow.com/questions/39395618/how-to-call-super-of-enclosing-class-in-a-mixin-in-python
            super().put(obj, block, timeout)  # type: ignore
        except queue.Full as ex:
            if self._name:
                raise queue.Full(f"Queue '{self._name}' is full") from ex
            else:
                raise

    @abstractmethod
    def discard_queued_items(self) -> None:
        """Pop and discard all items in the queue. Calls _discard_queued_items with an appropriate timeout."""
        raise NotImplementedError()

    def _discard_queued_items(self, pop_timeout: float) -> None:
        # Pop and discard all items in the queue. Called by discard_queued_items.
        while True:
            try:
                super().get(timeout=pop_timeout)  # type: ignore
            except queue.Empty:
                break
            except Exception as ex:
                print(f"{self._name}: Error while emptying buffer during cleanup: {safe_str(ex)}")  # Don't log, because this may be logging queue
                break


class ManagedThreadQueue(_ManagedQueueMixin, _ThreadQueue[T]):
    """A multi-threaded queue that can be used in a context-managed manner, provided for interface compatibility with ManagedProcessQueue.

    Also has an optional name, which is useful for debugging.
    """

    def __init__(self, maxsize: int = 0, name: Optional[str] = None) -> None:
        super().__init__(name=name, maxsize=maxsize)

    def __enter__(self) -> 'ManagedThreadQueue[T]':
        super().__enter__()
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        super().__exit__(exc_type, exc_value, traceback)

    def __getstate__(self) -> Dict[str, Any]:
        raise RuntimeError("ManagedThreadQueue must not be passed between processes. Use ManagedProcessQueue.")

    def discard_queued_items(self) -> None:
        self._discard_queued_items(pop_timeout=0.0)

    def __repr__(self) -> str:
        if self._name:
            return f"ManagedThreadQueue '{self._name}'"
        else:
            # noinspection PyCallByClass
            return object.__repr__(self)


class ManagedProcessQueue(_ManagedQueueMixin, _ProcessQueue[T]):
    """A context-managed cross-process queue. Cleanly closes down the queue when ending context management.

    Also has an optional name, which is useful for debugging.
    """

    def __init__(self, maxsize: int = 0, name: Optional[str] = None) -> None:
        super().__init__(name=name, maxsize=maxsize)

    def __enter__(self) -> 'ManagedProcessQueue[T]':
        super().__enter__()
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        super().__exit__(exc_type, exc_value, traceback)
        self._cleanup_queue()

    # noinspection PyTypeChecker
    @no_type_check
    def __getstate__(self) -> Dict[str, Any]:
        return {
            'super': super().__getstate__(),
            '_in_context_management': self._in_context_management,
            '_name': self._name
        }

    # noinspection PyTypeChecker
    @no_type_check
    def __setstate__(self, state: Dict[str, Any]) -> None:
        super().__setstate__(state['super'])
        self._in_context_management = state['_in_context_management']
        self._name = state['_name']

    def discard_queued_items(self) -> None:
        self._discard_queued_items(pop_timeout=PROCESS_DISCARD_TIMEOUT)

    def _cleanup_queue(self) -> None:
        """Cleans up a process queue when it is no longer required."""
        try:
            self.close()
            self.join_thread()  # wait for internal thread to die
        except Exception as ex:
            print(f"{self._name}: Error while cleaning up queue: {safe_str(ex)}")  # Not safe to log - this may be the logging queue!
            # Don't re-raise it, try to carry on

    def __repr__(self) -> str:
        if self._name:
            return f"ManagedProcessQueue '{self._name}'"
        else:
            # noinspection PyCallByClass
            return object.__repr__(self)


ManagedQueueTypes = Union[ManagedThreadQueue[T], ManagedProcessQueue[T]]
