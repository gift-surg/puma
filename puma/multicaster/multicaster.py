import logging
from typing import Generic, Set, TypeVar

from puma.buffer import Observable, Publishable
from puma.context import ensure_used_within_context_manager
from puma.multicaster._multicaster_subscriber import _MulticasterSubscriber
from puma.runnable import SingleBufferServicingRunnable
from puma.runnable.runner import ThreadRunner
from puma.unexpected_situation_action import UnexpectedSituationAction

Type = TypeVar("Type")

logger = logging.getLogger(__name__)


class Multicaster(ThreadRunner, Generic[Type]):
    """A thread which takes items out of one buffer and copies them to one or more subscribers (which are typically output buffers).

    Use the subscribe() and unsubscribe() methods to add and remove output buffers.
    Use start() to run the multicaster and join() to wait for it to end.

    The thread responds as soon as it is notified of an item being pushed, and so the buffer it is servicing will usually be emptied very rapidly, regardless of how quickly
    the subscribed buffers are being serviced.

    The behaviour of the multicaster if it cannot push an item to an output buffer (because it is full) depends on the option given when that buffer was subscribed.

    The thread ends when either the runnable receives the stop command on its command buffer, or has received on_complete from its input buffer, or an exception occurs
    in the thread itself.
    When ended, the runnable sends on_complete (including the thread's error, if any) to all output buffers that it has not already tried to send it to. If ending because
    of an exception in the thread, and on_complete has already been sent to all output buffers, then the exception is raised, which will be reported when the user polls for
    errors on the multicaster. This behaviour can be modified by overloading the _execution_ending_hook.

    The owner should regularly poll check_for_exceptions to receive errors raised by the thread, which will be passed back to the owner on its status buffer.
    """

    def __init__(self, observable: Observable[Type]) -> None:
        """Constructor. Use context management to control the thread's lifetime. Poll check_for_exceptions to receive errors raised by the thread.

        observable: The buffer whose items are to be copied to subscribers.
        """
        if not observable:
            raise ValueError("Observable must be supplied")
        name = f"Multicaster from '{observable.buffer_name()}'"
        self._subscriber: _MulticasterSubscriber[Type] = _MulticasterSubscriber(name)
        self._runnable: SingleBufferServicingRunnable[Type] = SingleBufferServicingRunnable(observable, self._subscriber, [], name)
        self._publishables: Set[Publishable[Type]] = set()
        super().__init__(runnable=self._runnable, name=name)

    def __enter__(self) -> 'Multicaster[Type]':
        super().__enter__()
        return self

    @ensure_used_within_context_manager
    def subscribe(self, publishable: Publishable[Type], on_full_action: UnexpectedSituationAction = UnexpectedSituationAction.RAISE_EXCEPTION) -> None:
        """Subscribe a publishable to receive a copy of items received from the buffer passed to the constructor.

        publishable: The subscribing buffer.
        on_full_action: Specifies the action that the multicaster will take if the supplied buffer is full when the multicaster tries to push an item on to it.
                        In the case of the RAISE_EXCEPTION option, queue.Full will be raised, which will be communicated to the owning thread on its status channel
                        (poll check_for_exceptions to receive errors).
        """
        if not publishable:
            raise ValueError("Publishable must be supplied")
        if publishable in self._publishables:
            raise RuntimeError("Publishable is already subscribed")
        self._publishables.add(publishable)
        publisher = self._runnable.multicaster_accessor.add_output_buffer(publishable)
        self._subscriber.subscribe(publisher, on_full_action)

    def unsubscribe(self, publishable: Publishable[Type]) -> None:
        """Unsubscribe a publisher. It is not necessary to call this before destroying the multicaster."""
        if not publishable:
            raise ValueError("Publishable must be supplied")
        if publishable not in self._publishables:
            raise RuntimeError("Publishable is not subscribed")
        self._publishables.remove(publishable)
        publisher = self._runnable.multicaster_accessor.remove_output_buffer(publishable)
        self._subscriber.unsubscribe(publisher)
