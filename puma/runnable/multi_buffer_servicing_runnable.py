import logging
import queue
import threading
import time
from abc import ABC
from contextlib import ExitStack
from typing import Any, Collection, Dict, List, Optional, Tuple, TypeVar, Union

from puma.attribute import copied, factory, unmanaged
from puma.buffer import Observable, Publishable, Subscriber, Subscription
from puma.helpers.string import safe_str
from puma.precision_timestamp.precision_timestamp import precision_timestamp
from puma.primitives import HighPrecisionAutoResetEvent
from puma.runnable import Runnable
from puma.runnable.decorator.run_in_child_scope import run_in_child_scope
from puma.runnable.message import CommandMessage
from puma.timeouts import Timeouts

logger = logging.getLogger(__name__)

T = TypeVar("T")


class MultiBufferServicingRunnable(Runnable, ABC):
    """Base class for Runnables that service values arriving on one or more input buffers, passing them on to associated Subscribers to be processed.

    If an error is received with on_complete, it is passed to the relevant subscriber.
    Also services the Runnable's command buffer.
    The _execute() method ends when either the runnable receives the stop command, or has received on_complete from all of its input buffers, or an exception occurs
    in the runnable itself.
    When ended, the runnable sends on_complete (including the runnable's error, if any) to all subscriptions that have not already received on_complete. If ending because
    of an exception in the runnable, and all subscriptions have already received on_complete, then the exception is raised, which will be reported when the user polls for errors
    on the runner which is running it. This behaviour can be modified by overloading the _execution_ending_hook.

    The owner should regularly poll check_for_exceptions to receive errors raised by the thread, which will be passed back to the owner on its status buffer.

    This class is abstract. Typically, a derived class will call _add_subscription to set up its inputs, in its constructor.

    This base class can also be configured to call a method, _on_tick(), at regular intervals.
    """
    _observables: List[Observable[Any]] = unmanaged("_observables")
    _subscribers: List[Subscriber[Any]] = unmanaged("_subscribers")
    _observables_completed: Dict[Observable[Any], bool] = unmanaged("_observables_completed")
    _subscribers_completed: Dict[Subscriber[Any], bool] = unmanaged("_subscribers_completed")
    _event: HighPrecisionAutoResetEvent = copied("_event")
    _executing: bool = copied("_executing")
    _tick_interval: Optional[float] = copied("_tick_interval")
    _next_tick_time: Optional[float] = copied("_next_tick_time")
    _tick_lock: threading.Lock = copied("_tick_lock")

    def __init__(self, name: str, output_buffers: Collection[Publishable[Any]], *, tick_interval: Union[int, float, None] = None) -> None:
        """Constructor.

        Arguments:
            name:            A name for the Runnable, used for logging.
            output_buffers:  The outputs that this runnable has. Use get_publisher to publish values.
            tick_interval:   If specified, the _on_tick() method will be called at this interval (seconds), after resume_ticks() has been called.
                             This interval can be changed later using set_tick_interval().
        """
        super().__init__(name, output_buffers)
        self._observables = []
        self._subscribers = []
        self._observables_completed = {}
        self._subscribers_completed = {}
        self._event = factory(HighPrecisionAutoResetEvent)
        self._executing = False
        self._tick_interval = None
        self._next_tick_time = None
        self._event = factory(HighPrecisionAutoResetEvent)
        self._tick_lock = factory(threading.Lock)
        if tick_interval is not None:
            self._set_tick_interval(tick_interval)

    def _add_subscription(self, observable: Observable[T], subscriber: Subscriber[T]) -> None:
        """ Registers an Observable to be serviced, with events being passed to the given Subscriber.

        It is legal to pass the same subscriber for multiple observables.
        It is illegal to add the same observable more than once.

        This method is protected rather than public because it is intended to be used by derived classes (typically in their constructors),
        not called from outside the class.
        """
        if self._executing:
            raise RuntimeError("Can't add a subscription while the runnable is executing")
        if not observable:
            raise ValueError("Observable must be supplied")
        if not subscriber:
            raise ValueError("Subscriber must be supplied")
        if observable in self._observables:
            raise RuntimeError("Observable is already present")
        logger.debug("Adding subscription: observable '%s' -> subscriber '%d'", observable.buffer_name(), len(self._subscribers))
        self._observables_completed[observable] = False
        if subscriber not in self._subscribers:
            self._subscribers_completed[subscriber] = False
        self._observables.append(observable)
        self._subscribers.append(subscriber)

    def _remove_subscription(self, observable: Observable[T], subscriber: Subscriber[T]) -> None:
        """ De-registers an Observable from being be serviced. Cannot be called while the runnable is executing.

        Calling this method is rarely necessary. It is *not* normally necessary to match calls to _add_subscription as part of clean-up.
        """
        if self._executing:
            raise RuntimeError("Can't remove a subscription while the runnable is executing")
        if not observable:
            raise ValueError("Observable must be supplied")
        if not subscriber:
            raise ValueError("Subscriber must be supplied")
        logger.debug("Removing subscription: observable '%s' -> subscriber '%d'", observable.buffer_name(), len(self._subscribers))
        try:
            index = self._observables.index(observable)
        except ValueError:
            raise RuntimeError("Observable is not present")
        if self._subscribers[index] != subscriber:
            raise RuntimeError("Given subscriber does not match that registered for the given observable")

        self._observables.pop(index)
        self._subscribers.pop(index)
        del self._observables_completed[observable]
        if subscriber not in self._subscribers:
            del self._subscribers_completed[subscriber]

    @run_in_child_scope
    def set_tick_interval(self, tick_interval: Union[int, float]) -> None:
        self._set_tick_interval(tick_interval)

    def _set_tick_interval(self, tick_interval: Union[int, float]) -> None:
        """Set or modify the tick interval."""
        with self._tick_lock:
            if tick_interval is None or tick_interval <= 0.0:
                raise ValueError("Tick interval must be greater than zero")
            if self._next_tick_time is not None:
                # Already ticking. Adjust the next tick time.
                assert self._tick_interval
                last_tick_time = self._next_tick_time - self._tick_interval
                self._next_tick_time = last_tick_time + tick_interval
            self._tick_interval = tick_interval
            # because this method is handled as a command, we don't need to set the event, even if shortening the interval

    @run_in_child_scope
    def resume_ticks(self) -> None:
        """Start or resume ticking. This must be called for ticking to begin. The first tick occurs one interval after this call.

        Has no effect if ticking is already in progress.
        """
        with self._tick_lock:
            if self._tick_interval is None:
                raise RuntimeError("Trying to start regular ticking without having set the tick interval")
            if self._next_tick_time is not None:
                return  # already ticking
            # The next tick time is one interval from now
            self._next_tick_time = time.perf_counter() + self._tick_interval
            # because this method is handled as a command, we don't need to set the event

    @run_in_child_scope
    def pause_ticks(self) -> None:
        """Pause ticking."""
        with self._tick_lock:
            self._next_tick_time = None

    def _execute(self) -> None:
        # Runs in a separate thread or process, servicing the input and command buffers. See comments in Runnable interface.
        logger.debug("%s: Running", self._name)
        self._executing = True
        try:
            self._check_ready_to_execute()
            with ExitStack() as stack:
                work_subscriptions = [stack.enter_context(obs.subscribe(self._event)) for obs in self._observables]
                command_subscription = stack.enter_context(self._get_command_message_buffer().subscribe(self._event))
                try:
                    while self._should_continue():
                        self._pre_wait_hook()
                        self.__wait_on_event(self._interval_to_next_tick())
                        self.__tick_if_due()
                        self.__service_command_buffer(command_subscription)
                        self.__service_input_buffers(work_subscriptions)
                except Exception as ex:
                    self._execution_ending(ex)
                else:
                    self._execution_ending(None)
        finally:
            self._executing = False
            logger.debug("%s: Finished", self._name)

    def _on_tick(self, timestamp: float) -> None:
        """Derived classes should implement this method to make use of the regular ticking facility of the runnable.

        The given timestamp has a precision of at least one millisecond and is system wide (comparable between threads and processes). it is the time
        'now', not the nominal time at which the tick should have been scheduled.
        """
        pass

    def _pre_wait_hook(self) -> None:
        """Placeholder for derived classes to do some work each time before the runner (potentially) goes to sleep."""
        return None

    def _execution_ending_hook(self, error: Optional[Exception]) -> bool:
        """Called when the runnable is about to end, having sent on_complete to all subscribers that hadn't already been sent it.

        Returns true if the error (if there is one) has been handled.
        Derived classes will typically override this if they have some way to communicate the error, e.g. they have some output buffer that can take it, and return true.
        If the error is not None, and it was not sent to any subscribers, and this method returns False, then the error is raised.

        If this method raises an exception and error was None, then the exception will be raised.
        If this method raises an exception and error was not None, the exception is logged and the original error will be raised.
        """
        return False

    def _execution_ending(self, error: Optional[Exception]) -> None:
        """Called when the runnable is about to exit its loop"""
        error_has_been_sent_to_some_subscribers, error = self._ensure_all_subscribers_have_been_completed(error)
        error_handled_by_hook, error = self._call_execution_ending_hook(error)
        if error and not error_has_been_sent_to_some_subscribers and not error_handled_by_hook:
            raise error

    def _ensure_all_subscribers_have_been_completed(self, error: Optional[Exception]) -> Tuple[bool, Optional[Exception]]:
        # Returns a tuple consisting of:
        #   a boolean indicating whether an error was sent to any of the subscribers
        #   an updated error value, if there was no error originally but an error occurred in some on_complete call
        logger.debug("%s: Finishing - ensuring all subscribers have received Complete", self._name)

        uncompleted_subscribers = [subscriber for subscriber, already_completed in self._subscribers_completed.items() if not already_completed]
        error_sent_to_some_subscriber = False
        for subscriber in uncompleted_subscribers:
            error_sent_to_subscriber, error = self._send_final_on_complete_to_subscriber(error, subscriber)
            if error_sent_to_subscriber:
                error_sent_to_some_subscriber = True

        return error_sent_to_some_subscriber, error

    def _send_final_on_complete_to_subscriber(self, error: Optional[Exception], subscriber: Subscriber) -> Tuple[bool, Optional[Exception]]:
        error_sent_to_subscriber = False
        try:
            subscriber.on_complete(error)
        except Exception as ex:
            if error:
                if ex == error:
                    logger.debug(f"The error '{safe_str(ex)}' was re-raised by a subscriber in on_complete when the runnable was ending. "
                                 f"The runnable will continue handling the error.")
                else:
                    logger.warning(f"An error '{safe_str(ex)}' was raised by a subscriber in on_complete (with an existing error '{error}') when the runnable was ending. "
                                   f"The runnable will continue handling the original error.")
            else:
                # from now on, we are dealing with this error situation
                logger.error(f"An error '{safe_str(ex)}' occurred while sending on_complete to a subscriber.")
                error = ex
        else:
            self._subscribers_completed[subscriber] = True
            if error:
                error_sent_to_subscriber = True
        return error_sent_to_subscriber, error

    def _call_execution_ending_hook(self, error: Optional[Exception]) -> Tuple[bool, Optional[Exception]]:
        # Returns a tuple consisting of:
        #   a boolean indicating whether an error was handled by the hook
        #   an updated error value, if there was no error originally but an error occurred in the hook
        error_handled_by_hook = False
        try:
            handled_by_hook = self._execution_ending_hook(error)
        except Exception as ex:
            if error:
                if ex == error:
                    logger.debug(f"The error '{safe_str(ex)}' was re-raised in the execution-ending-hook. The runnable will "
                                 f"continue handling the error.")
                else:
                    logger.warning(f"An error '{safe_str(ex)}' occurred in the execution-ending-hook while trying to handle an existing error '{error}'. The runnable will "
                                   f"continue handling the original error.")
            else:
                # from now on, we are dealing with this error situation
                logger.error(f"The execution-ending-hook raised exception '{safe_str(ex)}'. The runnable will raise this error.")
                error = ex
        else:
            if error and handled_by_hook:
                error_handled_by_hook = True

        return error_handled_by_hook, error

    def __service_input_buffers(self, work_subscriptions: List[Subscription[Any]]) -> None:
        for i in range(len(self._subscribers)):
            subscription = work_subscriptions[i]
            observable = self._observables[i]
            subscriber = self._subscribers[i]
            observable_name = observable.buffer_name()
            logger.debug("%s: Polling input buffer '%s'", self._name, observable_name)
            while self._should_continue():
                try:
                    subscription.call_events(subscriber.on_value, lambda error: self._on_complete(error, subscriber, observable, i))
                except queue.Empty:
                    logger.debug("%s: Input buffer '%s' now empty", self._name, observable_name)
                    break

    def __service_command_buffer(self, command_subscription: Subscription[Any]) -> None:
        logger.debug("%s: Polling command buffer", self._name)
        while self._should_continue():
            try:
                command_subscription.call_events(self._on_command)
            except queue.Empty:
                logger.debug("%s: Commands buffer now empty", self._name)
                break

    def __wait_on_event(self, event_timeout: Optional[float]) -> None:
        Timeouts.validate_optional(event_timeout)
        logger.debug("%s: Sleeping for %s", self._name, Timeouts.describe_optional(event_timeout))
        if self._event.wait(event_timeout):
            logger.debug("%s: Woken", self._name)
        else:
            logger.debug("%s: Timed out", self._name)

    def _on_command(self, value: CommandMessage) -> None:
        # Called by _execute() when a command message is received
        logger.debug("%s: Got command %s from command buffer", self._name, str(value))
        self._handle_command(value)  # Runnable base class default command handling; in the case of the STOP command, sets self._stop_task

    def _on_complete(self, error: Optional[BaseException], subscriber: Subscriber[Any], observable: Observable[Any], subscriber_index: int) -> None:
        # Called by _execute() when on_complete is received from an input buffer
        logger.debug("%s: Got Complete (with error '%s') from input buffer '%s'", self._name, safe_str(error), observable.buffer_name())
        self._observables_completed[observable] = True
        if error:
            logger.debug("Raising received error, ending the runnable")
            raise error
            # The error will be sent to all subscribers that have not already been completed, including this one, if possible

        if self._subscribers_completed[subscriber]:
            logger.debug("Not sending on_complete to subscriber %d because it has already been sent it", subscriber_index)
        else:
            logger.debug("Sending on_complete to subscriber %d", subscriber_index)
            try:
                subscriber.on_complete(None)
            except Exception as ex:
                logger.warning(f"Error '{safe_str(ex)}' was raised by a subscriber in on_complete. The runnable will end.")
                raise
            else:
                self._subscribers_completed[subscriber] = True

    def _should_continue(self) -> bool:
        # Returns true if the _execute method should continue running
        return not (self._stop_task or self._all_observables_completed())

    def _all_observables_completed(self) -> bool:
        return all(self._observables_completed.values())

    def _check_ready_to_execute(self) -> None:
        super()._check_ready_to_execute()
        self._check_observables_set()

    def _check_observables_set(self) -> None:
        if not self._observables:
            raise RuntimeError("At least one subscription must be added before executing")

    def _interval_to_next_tick(self) -> Optional[float]:
        # Returns the timeout for waiting on the event
        with self._tick_lock:
            if not self._next_tick_time:
                return None  # wait on the event forever

            now = time.perf_counter()
            ret = max(0.0, self._next_tick_time - now)
            logger.debug("Next tick time: %0.3f -> Sleep for %0.3f", self._next_tick_time, ret)
            return ret

    def __tick_if_due(self) -> None:
        # Calls _on_tick() if its time is due, and if so also advances the next tick time
        with self._tick_lock:
            if self._next_tick_time is None:
                # Not ticking
                return

            now = time.perf_counter()
            if now < self._next_tick_time:
                logger.debug("Tick not due until %0.3f", self._next_tick_time)
                return

            logger.debug("Ticking now - was due at %0.3f", self._next_tick_time)

            assert self._tick_interval
            self._next_tick_time += self._tick_interval
            if self._next_tick_time < now:
                # we have got behind, don't try to make every missed call
                self._next_tick_time = now + self._tick_interval
                logger.debug("Ticking has got behind; next tick time advanced to %0.3f", self._next_tick_time)
            else:
                logger.debug("Next tick time advanced to %0.3f", self._next_tick_time)

        # call on_tick, outside the lock
        self._on_tick(precision_timestamp())
