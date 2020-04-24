import queue
import threading
from collections import deque
from typing import Any, Callable, Deque, Mapping, Optional, Set, TypeVar, Union

from puma.buffer import Buffer, DEFAULT_PUBLISH_COMPLETE_TIMEOUT, DEFAULT_PUBLISH_VALUE_TIMEOUT, OnComplete, OnValue, Publisher, Subscriber, Subscription
from puma.buffer.internal.items.complete_item import CompleteItem
from puma.buffer.internal.items.queue_item import QueueItem
from puma.buffer.internal.items.value_item import ValueItem
from puma.context import Exit_1, Exit_2, Exit_3, ensure_used_within_context_manager
from puma.primitives import AutoResetEvent
from puma.runnable import Runnable
from puma.runnable.message import StatusMessage
from puma.runnable.runner import Runner
from puma.unexpected_situation_action import UnexpectedSituationAction, handle_unexpected_situation

BufferType = TypeVar("BufferType")
ABufferType = TypeVar("ABufferType")


# The types in this module are intended to "simulate" multi-threaded or multi-process primitives in a single-threaded manner.
# They are intended only for testing.


class InlineEvent:
    """A dummy implementation of Event. Wait never blocks since in a single-threaded context this would always deadlock."""

    def __init__(self) -> None:
        self._flag = False

    def is_set(self) -> bool:
        return self._flag

    def set(self) -> None:
        self._flag = True

    def clear(self) -> None:
        self._flag = False

    def wait(self, timeout: Union[int, float, None] = None) -> bool:
        return self._flag


class InlineCondition:
    """A dummy implementation of Condition. Since wait can never block in a single-threaded context, without causing a deadlock, this effectively does nothing."""

    def __init__(self, lock: Union[threading.Lock, threading.RLock, None] = None) -> None:
        self._is_owned = False

    def __enter__(self) -> 'InlineCondition':
        self._is_owned = True
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        self._is_owned = False

    def wait(self, timeout: Union[int, float, None] = None) -> bool:
        if not self._is_owned:
            raise RuntimeError("cannot wait on un-acquired lock")
        return False

    def wait_for(self, predicate: Callable[[], bool], timeout: Union[int, float, None] = None) -> bool:
        return predicate()

    def notify(self, n: int = 1) -> None:
        if not self._is_owned:
            raise RuntimeError("cannot notify on un-acquired lock")

    def notify_all(self) -> None:
        if not self._is_owned:
            raise RuntimeError("cannot notify on un-acquired lock")


class InlineBuffer(Buffer[BufferType]):
    """An implementation of Buffer that is non-blocking (has no locking or timeouts), is instantaneous, and is not necessarily thread or process safe."""

    def __init__(self, size: int, name: str) -> None:
        self._name = name
        self.values: Deque[QueueItem] = deque(maxlen=size)
        self.publishers: Set[Publisher[BufferType]] = set()
        self.subscription: Optional[Subscription[BufferType]] = None
        self._event: Optional[AutoResetEvent] = None

    def publish(self) -> Publisher[BufferType]:
        publisher: Publisher[BufferType] = _InlinePublisher(self._name, self.values, self._event)
        self.publishers.add(publisher)
        return publisher

    def unpublish(self, publisher: Publisher[BufferType]) -> None:
        self.publishers.remove(publisher)

    def subscribe(self, event: Optional[AutoResetEvent]) -> Subscription[BufferType]:
        self.subscription = _InlineSubscription(self._name, self.values)
        for publisher in self.publishers:
            publisher.set_subscriber_event(event)
        self._event = event
        if event and len(self.values) > 0:
            event.set()
        return self.subscription

    def unsubscribe(self) -> None:
        self.subscription = None
        self._event = None
        for publisher in self.publishers:
            publisher.set_subscriber_event(None)

    def buffer_name(self) -> str:
        return self._name


class _InlinePublisher(Publisher[BufferType]):
    """Returned by TestInlineBuffer.publish(), provides the Publisher end of the buffer"""

    def __init__(self, name: str, values: Deque[QueueItem], subscriber_event: Optional[AutoResetEvent]) -> None:
        self._name = name
        self.values = values
        self._event = subscriber_event

    def __enter__(self) -> 'Publisher[BufferType]':
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        pass

    def publish_value(self, value: BufferType, timeout: float = DEFAULT_PUBLISH_VALUE_TIMEOUT,
                      on_full_action: UnexpectedSituationAction = UnexpectedSituationAction.RAISE_EXCEPTION) -> None:
        self._push(ValueItem(value), on_full_action)

    def publish_complete(self, error: Optional[BaseException], timeout: float = DEFAULT_PUBLISH_COMPLETE_TIMEOUT,
                         on_full_action: UnexpectedSituationAction = UnexpectedSituationAction.RAISE_EXCEPTION) -> None:
        self._push(CompleteItem(error), on_full_action)

    def buffer_name(self) -> str:
        return self._name

    def invalidate(self) -> None:
        pass

    def set_subscriber_event(self, subscriber_event: Optional[AutoResetEvent]) -> None:
        self._event = subscriber_event

    def _push(self, item: QueueItem, on_full_action: UnexpectedSituationAction) -> None:
        if len(self.values) == self.values.maxlen:
            self._handle_buffer_full_exception(on_full_action)
        if self._event:
            self.values.append(item)
            self._event.set()
        else:
            self.values.append(item)

    def _handle_buffer_full_exception(self, on_full_action: UnexpectedSituationAction) -> None:
        # Utility method for use by derived classes, to gracefully handle the buffer full condition
        handle_unexpected_situation(on_full_action, f"{self._name}: Buffer full", None,
                                    exception_factory=lambda s: queue.Full(s))  # if on_full_action=RAISE_EXCEPTION, re-raise queue.Full rather than RuntimeError


class _InlineSubscription(Subscription[BufferType]):
    """Returned by TestInlineBuffer.subscribe(), provides the Subscription end of the buffer"""

    def __init__(self, name: str, values: Deque[QueueItem]) -> None:
        self._name = name
        self._values = values

    def __enter__(self) -> 'Subscription[BufferType]':
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        pass

    def call_events(self, on_value_or_subscriber: Union[OnValue[BufferType], Subscriber[BufferType]], on_complete: Optional[OnComplete] = None) -> None:
        try:
            item = self._values.popleft()
        except IndexError:
            raise queue.Empty
        self._handle_item(item, on_value_or_subscriber, on_complete)

    def invalidate(self) -> None:
        pass

    def buffer_name(self) -> str:
        return self._name

    def _handle_item(self, item: QueueItem, on_value_or_subscriber: Union[OnValue[BufferType], Subscriber[BufferType]], on_complete: Optional[OnComplete]) -> None:
        if isinstance(on_value_or_subscriber, Subscriber):
            if on_complete:
                raise ValueError(f"{self._name}: on_complete may only be provided if on_value_or_subscriber is a callback function, not a Subscriber")
            subscriber = on_value_or_subscriber
            self._handle_item_callbacks(item, subscriber.on_value, subscriber.on_complete)
        elif callable(on_value_or_subscriber):
            on_value = on_value_or_subscriber
            self._handle_item_callbacks(item, on_value, on_complete)
        elif on_value_or_subscriber is None:
            raise TypeError("on_value_or_subscriber must not be None")
        else:
            raise TypeError("on_value_or_subscriber is not of the correct type")

    def _handle_item_callbacks(self, item: QueueItem, on_value: OnValue[BufferType], on_complete: Optional[OnComplete]) -> None:
        if isinstance(item, ValueItem):
            on_value(item.value)
        elif isinstance(item, CompleteItem):
            if on_complete:
                on_complete(item.get_error())
        else:
            raise ValueError(f"{self._name}: Invalid QueueItem received: {item}")


class InlineRunner(Runner):
    """An implementation of Runner that does not launch a separate thread or process but instead executes the runnable in the caller's thread,
    and behaves as if it has instantaneous command and status buffers.
    """

    def __init__(self, runnable: Runnable, name: Optional[str] = None) -> None:
        self._name = ""
        self._runnable = runnable
        self.wrapped_status_buffer: Optional[InlineBuffer] = None
        self.started = False
        self.stopped = False
        super().__init__(runnable, name)  # should call our set_name() and hence set self._name

    def __enter__(self) -> 'InlineRunner':
        super().__enter__()
        return self

    @ensure_used_within_context_manager
    def start(self) -> None:
        self._runnable.runner_accessor.assert_comms_not_already_set()
        self._runnable.runner_accessor.set_command_buffer(self._command_buffer)
        self._runnable.runner_accessor.set_status_buffer_subscription(self._status_buffer_subscription)
        self.started = True
        super().run()

    def _perform_join(self, timeout: Optional[float] = None) -> None:
        pass

    @ensure_used_within_context_manager
    def stop(self) -> None:
        super().stop()
        self.stopped = True

    def is_alive(self) -> bool:
        return self.started and not self.stopped

    def get_name(self) -> str:
        return self._name

    def set_name(self, name: str) -> None:
        self._name = name

    def _buffer_factory(self, element_type: ABufferType, size: int, name: str, warn_on_discard: Optional[bool] = True) -> Buffer[ABufferType]:
        return InlineBuffer[ABufferType](size, name)

    def _create_status_message_buffer(self) -> InlineBuffer[StatusMessage]:
        self.wrapped_status_buffer = InlineBuffer[StatusMessage](self._get_command_and_status_buffer_size(), "Status buffer on " + self.get_name())
        return self.wrapped_status_buffer


class InlineExecutor:
    """Looks like a thread or process, but actually just executes the target in the current thread."""

    def __init__(self, name: str, target: Optional[Callable[..., Any]] = None, kwargs: Optional[Mapping[str, Any]] = None) -> None:
        self._name = str(name or "")
        self._target = target
        self._kwargs = kwargs

    def start(self) -> None:
        self.run()

    def run(self) -> None:
        if self._target:
            self._target(kwargs=self._kwargs)

    def join(self, timeout: Union[int, float, None] = None) -> None:
        pass

    @property
    def name(self) -> str:
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        self._name = str(name)

    def is_alive(self) -> bool:
        return False
