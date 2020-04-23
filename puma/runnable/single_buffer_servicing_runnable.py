import logging
from abc import ABC
from typing import Any, Collection, Generic, TypeVar, Union

from puma.buffer import Observable, Publishable, Subscriber
from puma.runnable import MultiBufferServicingRunnable

Type = TypeVar("Type")

logger = logging.getLogger(__name__)


class SingleBufferServicingRunnable(Generic[Type], MultiBufferServicingRunnable, ABC):
    """Base class for Runnables that service values arriving on a single input buffer, passing them on to a given Subscriber to be processed.

    If an error is received with on_complete, it is passed to the subscriber.
    Also services the Runnable's command buffer.
    The _execute() method ends when either the runnable receives the stop command, or has received on_complete from its input buffer, or an exception occurs
    in the runnable itself.
    When ended, the runnable sends on_complete (including the runnable's error, if any) to the subscriber if it has not already received on_complete. If ending because
    of an exception in the runnable, and the subscription has already received on_complete, then the exception is raised, which will be reported when the user polls for errors
    on the runner which is running it. This behaviour can be modified by overloading the _execution_ending_hook.

    The owner should regularly poll check_for_exceptions to receive errors raised by the thread, which will be passed back to the owner on its status buffer.
    """

    def __init__(self, observable: Observable[Type], subscriber: Subscriber[Type], output_buffers: Collection[Publishable[Any]], name: str,
                 *,
                 tick_interval: Union[int, float, None] = None) -> None:
        """Constructor.

        Arguments:
            observable:      The input buffer being serviced.
            subscriber:      An object that processes the values and events received from the observable.
            output_buffers:  The outputs that this runnable has. Use get_publisher to publish values.
            name:            A name for the Runnable, used for logging.
            tick_interval:   If specified, the _on_tick() method will be called at this interval, after resume_ticks() has been called.
                             This interval can be changed later using set_tick_interval().

        When handling calls to the subscriber's on_value and on_complete, user code should raise an error if it cannot service the call (for example, if a buffer is full).
        In the case of on_complete, it is fine to raise some new error (such as queue.Full), you do not have to re-raise the error you are given, if there is one.
        The Runnable will treat the error you raise as fatal, ending the Runner and the Runnable. The error will be raised by the Runner's check_for_exceptions call.
        However if you raise an error in on_complete when an error is already being handled (if on_complete's error parameter is not None), your error will be logged
        but otherwise ignored and the runnable will raise the original error.
        """
        super().__init__(name, output_buffers, tick_interval=tick_interval)
        self._add_subscription(observable, subscriber)
