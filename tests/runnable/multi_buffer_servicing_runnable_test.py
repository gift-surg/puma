import logging
import time
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Thread
from typing import Any, Callable, Collection, List, Optional, TypeVar, no_type_check
from unittest import TestCase

from puma.attribute import copied, parent_only
from puma.buffer import Observable, Publishable, Subscriber
from puma.helpers.testing.logging.decorator import assert_no_warnings_or_errors_logged
from puma.helpers.testing.mixin import NotATestCase
from puma.runnable import MultiBufferServicingRunnable
from puma.runnable.message import CommandMessage, StartedStatusMessage, StatusBuffer, StatusMessage
from puma.timeouts import TIMEOUT_NO_WAIT
from puma.unexpected_situation_action import UnexpectedSituationAction
from tests.buffer.test_support.buffer_api_test_support import TestSubscriber
from tests.buffer.test_support.test_inline_buffer import TestInlineBuffer
from tests.runnable.test_support.call_runnable_method_on_running_instance import call_runnable_method_on_running_instance

logger = logging.getLogger(__name__)

T = TypeVar("T")

DELAY = 0.5
TIME_TOLERANCE = 0.3


@dataclass(frozen=True)
class AcceptedStatusCommand(CommandMessage):
    """A command that is accepted by TestMultiBufferServicingRunnable"""


class TestMultiBufferServicingRunnable(MultiBufferServicingRunnable, NotATestCase):
    # Test class for CommandDrivenRunnable. Each time round its loop (when a command is received, unless a timeout is specified), _pre_wait_hook gets called
    # and this calls out to a user-supplied test function. This function is given a counter so it can perform a series of test actions.
    _test_callable: Callable[['TestMultiBufferServicingRunnable', int], None] = parent_only("_test_callable")
    _immortal: bool = parent_only("_immortal")
    _handle_in_ending_hook: bool = parent_only("_handle_in_ending_hook")
    call_count: int = copied("call_count")
    loop_times: List[float] = parent_only("loop_times")
    special_command_received: bool = parent_only("special_command_received")

    def __init__(self,
                 test_callable: Callable[['TestMultiBufferServicingRunnable', int], None],
                 output_buffers: Collection[Publishable[Any]],
                 *,
                 immortal: bool = False,
                 handle_in_ending_hook: bool = False) -> None:
        super().__init__("Test runnable", output_buffers)
        # MyPy complains about assigning to a method: https://github.com/python/mypy/issues/708
        self._test_callable = test_callable  # type: ignore
        self._immortal = immortal
        self._handle_in_ending_hook = handle_in_ending_hook
        self.call_count = 0
        self.loop_times = []
        self.special_command_received = False

    def add_subscription(self, observable: Observable[T], subscriber: Subscriber[T]) -> None:
        self._add_subscription(observable, subscriber)

    def remove_subscription(self, observable: Observable[T], subscriber: Subscriber[T]) -> None:
        self._remove_subscription(observable, subscriber)

    def send_command(self, command: CommandMessage) -> None:
        self._send_command(command)

    def _pre_wait_hook(self) -> None:
        self.loop_times.append(time.perf_counter())
        # MyPy again complains erroneously: https://github.com/python/mypy/issues/708
        self._test_callable(self, self.call_count)  # type: ignore
        self.call_count += 1

    def _handle_command(self, command: CommandMessage) -> None:
        if isinstance(command, AcceptedStatusCommand):
            self.special_command_received = True
        else:
            super()._handle_command(command)

    def _should_continue(self) -> bool:
        if self._immortal:
            return True
        return super()._should_continue()

    def _execution_ending_hook(self, error: Optional[Exception]) -> bool:
        return self._handle_in_ending_hook


class _MultiBufferTestSubscriber(TestSubscriber):
    def __init__(self) -> None:
        super().__init__()
        self.error_in_on_complete = 0
        self.error_in_on_complete_reraise = False
        self._errored_count = 0

    def on_complete(self, error: Optional[BaseException]) -> None:
        if self._errored_count < self.error_in_on_complete:
            self._errored_count += 1
            if error and self.error_in_on_complete_reraise:
                raise error
            else:
                raise RuntimeError(f"Error in on_complete {self._errored_count}")
        super().on_complete(error)


class IllegalParamsTestRunnable(MultiBufferServicingRunnable):

    def _all_observables_completed(self) -> bool:
        return False

    def _check_observables_set(self) -> None:
        # Do nothing
        pass


class MultiBufferServicingRunnableTest(TestCase):
    def __init__(self, name: str) -> None:
        super().__init__(name)
        self._input_buffer_1: TestInlineBuffer[str] = TestInlineBuffer[str](10, "Test input buffer 1")
        self._input_buffer_2: TestInlineBuffer[str] = TestInlineBuffer[str](10, "Test input buffer 2")
        self._input_buffer_3: TestInlineBuffer[str] = TestInlineBuffer[str](10, "Test input buffer 3")
        self._output_subscriber_1: _MultiBufferTestSubscriber = _MultiBufferTestSubscriber()
        self._output_subscriber_2: _MultiBufferTestSubscriber = _MultiBufferTestSubscriber()
        self._output_subscriber_3: _MultiBufferTestSubscriber = _MultiBufferTestSubscriber()
        self._thread: Optional[Thread] = None

    def setUp(self) -> None:
        self._input_buffer_1.__enter__()
        self._input_buffer_2.__enter__()
        self._input_buffer_3.__enter__()

    def tearDown(self) -> None:
        self._input_buffer_3.__exit__(None, None, None)
        self._input_buffer_2.__exit__(None, None, None)
        self._input_buffer_1.__exit__(None, None, None)
        # Individual tests that set self._thread should join on it
        self.assertFalse(self._thread and self._thread.is_alive())

    @assert_no_warnings_or_errors_logged
    def test_finishes_if_commanded_to_stop(self) -> None:
        # The runnable must stop when told to stop

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            if count < 100:
                with self._input_buffer_1.publish() as publisher:
                    publisher.publish_value(str(count))
            elif count == 100:
                the_runnable.stop()
            else:
                self.fail("Runnable should have stopped")

        self._run_runnable(actions)

        self._output_subscriber_1.assert_published_values([str(i) for i in range(100)], self)
        self._output_subscriber_2.assert_published_values([], self)
        self._output_subscriber_1.assert_completed(True, self)
        self._output_subscriber_2.assert_completed(True, self)
        self._output_subscriber_1.assert_error_values([], self)
        self._output_subscriber_2.assert_error_values([], self)

    @assert_no_warnings_or_errors_logged
    def test_in_thread_finishes_if_commanded_to_stop(self) -> None:
        # same as previous test but the runner is in a separate thread

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            if count < 100:
                with self._input_buffer_1.publish() as publisher:
                    publisher.publish_value(str(count))
            elif count == 100:
                the_runnable.stop()
            else:
                self.fail("Runnable should have stopped")

        self._run_runnable_in_thread(actions)

        self._output_subscriber_1.assert_published_values([str(i) for i in range(100)], self)
        self._output_subscriber_2.assert_published_values([], self)
        self._output_subscriber_1.assert_completed(True, self)
        self._output_subscriber_2.assert_completed(True, self)
        self._output_subscriber_1.assert_error_values([], self)
        self._output_subscriber_2.assert_error_values([], self)

    @assert_no_warnings_or_errors_logged
    def test_finishes_when_all_observables_finished(self) -> None:
        # The runnable must stop when all its input buffers have sent complete (if none of them contain an error)

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter

            # Buffer 1 counts to 50 then finishes
            if count < 50:
                with self._input_buffer_1.publish() as publisher:
                    publisher.publish_value(str(count))
            elif count == 50:
                with self._input_buffer_1.publish() as publisher:
                    publisher.publish_complete(error=None, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)

            # Buffer 2 counts to 100 then finishes
            if count < 100:
                with self._input_buffer_2.publish() as publisher:
                    publisher.publish_value(str(count))
            elif count == 100:
                with self._input_buffer_2.publish() as publisher:
                    publisher.publish_complete(error=None, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)

            # verify that on_complete was sent as it was received, not waiting for the runnable to end
            if count == 51:
                self._output_subscriber_1.assert_completed(True, self)
                self._output_subscriber_2.assert_completed(False, self)

            if count > 100:
                self.fail("Runnable should have stopped")

        runnable = self._run_runnable(actions)

        self.assertEqual(101, runnable.call_count)  # verifies that it didn't stop until it had received on_complete on both inputs
        self._output_subscriber_1.assert_published_values([str(i) for i in range(50)], self)
        self._output_subscriber_2.assert_published_values([str(i) for i in range(100)], self)
        self._output_subscriber_1.assert_completed(True, self)
        self._output_subscriber_2.assert_completed(True, self)
        self._output_subscriber_1.assert_error_values([], self)
        self._output_subscriber_2.assert_error_values([], self)

    @assert_no_warnings_or_errors_logged
    def test_in_thread_finishes_when_all_observables_finished(self) -> None:
        # same as previous test but the runner is in a separate thread

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter

            # Buffer 1 counts to 50 then finishes
            if count < 50:
                with self._input_buffer_1.publish() as publisher:
                    publisher.publish_value(str(count))
            elif count == 50:
                with self._input_buffer_1.publish() as publisher:
                    publisher.publish_complete(error=None, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)

            # Buffer 2 counts to 100 then finishes
            if count < 100:
                with self._input_buffer_2.publish() as publisher:
                    publisher.publish_value(str(count))
            elif count == 100:
                with self._input_buffer_2.publish() as publisher:
                    publisher.publish_complete(error=None, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)

            # verify that on_complete was sent as it was received, not waiting for the runnable to end
            if count == 51:
                self._output_subscriber_1.assert_completed(True, self)
                self._output_subscriber_2.assert_completed(False, self)

            if count > 100:
                self.fail("Runnable should have stopped")

        runnable = self._run_runnable_in_thread(actions)

        self.assertEqual(101, runnable.call_count)  # verifies that it didn't stop until it had received on_complete on both inputs
        self._output_subscriber_1.assert_published_values([str(i) for i in range(50)], self)
        self._output_subscriber_2.assert_published_values([str(i) for i in range(100)], self)
        self._output_subscriber_1.assert_completed(True, self)
        self._output_subscriber_2.assert_completed(True, self)
        self._output_subscriber_1.assert_error_values([], self)
        self._output_subscriber_2.assert_error_values([], self)

    @assert_no_warnings_or_errors_logged
    def test_finishes_if_exception_in_runnable_errors_sent_on_complete(self) -> None:
        # If an error occurs in the runnable then it should be sent out in an completed message

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter

            if count == 0:
                raise RuntimeError("Test error")
            else:
                self.fail("Runnable should have stopped")

        self._run_runnable(actions)

        self._output_subscriber_1.assert_completed(True, self)
        self._output_subscriber_2.assert_completed(True, self)
        self._output_subscriber_1.assert_error_values(["RuntimeError('Test error')"], self)
        self._output_subscriber_2.assert_error_values(["RuntimeError('Test error')"], self)

    @assert_no_warnings_or_errors_logged
    def test_finishes_if_exception_in_runnable_on_complete_already_sent_hook_does_not_handle(self) -> None:
        # If an error occurs in the runnable and all subscribers have already published complete and the _execution_ending_hook doesn't handle it then
        # the exception will get raised by _execute()

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter

            if count == 0:
                with self._input_buffer_1.publish() as publisher:
                    publisher.publish_complete(error=None, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
                with self._input_buffer_2.publish() as publisher:
                    publisher.publish_complete(error=None, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
                with self._input_buffer_3.publish() as publisher:
                    publisher.publish_complete(error=None, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            elif count == 1:
                raise RuntimeError("Test error")
            else:
                self.fail("Runnable should have stopped")

        with self.assertRaisesRegex(RuntimeError, "Test error"):
            self._run_runnable(actions, immortal=True, handle_in_ending_hook=False)

        self._output_subscriber_1.assert_completed(True, self)
        self._output_subscriber_2.assert_completed(True, self)
        self._output_subscriber_1.assert_error_values([], self)
        self._output_subscriber_2.assert_error_values([], self)

    @assert_no_warnings_or_errors_logged
    def test_finishes_if_exception_in_runnable_on_complete_already_sent_hook_handles(self) -> None:
        # If an error occurs in the runnable and all subscribers have already published complete but the _execution_ending_hook does handle it then
        # the exception will not get raised by _execute()
        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter

            if count == 0:
                with self._input_buffer_1.publish() as publisher:
                    publisher.publish_complete(error=None, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
                with self._input_buffer_2.publish() as publisher:
                    publisher.publish_complete(error=None, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
                with self._input_buffer_3.publish() as publisher:
                    publisher.publish_complete(error=None, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            elif count == 1:
                raise RuntimeError("Test error")
            else:
                self.fail("Runnable should have stopped")

        self._run_runnable(actions, immortal=True, handle_in_ending_hook=True)

        self._output_subscriber_1.assert_completed(True, self)
        self._output_subscriber_2.assert_completed(True, self)
        self._output_subscriber_1.assert_error_values([], self)
        self._output_subscriber_2.assert_error_values([], self)

    @assert_no_warnings_or_errors_logged
    def test_stopped_when_one_observable_already_completed(self) -> None:
        # When we are stopped we should publish complete onto any subscriber that hasn't already published it, but not to any subscriber that has already published it.

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            if count == 0:
                with self._input_buffer_1.publish() as publisher:
                    publisher.publish_complete(error=None, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            elif count == 1:
                the_runnable.stop()
            else:
                self.fail("Runnable should have stopped")

        self._run_runnable(actions)

        self._output_subscriber_1.assert_completed(True, self)
        self._output_subscriber_2.assert_completed(True, self)
        self._output_subscriber_1.assert_error_values([], self)
        self._output_subscriber_2.assert_error_values([], self)

    @assert_no_warnings_or_errors_logged
    def test_received_error_is_passed_on(self) -> None:
        # If we receive an error in publish_complete on one input, we pass that error on to all subscribers and stop.

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter

            if count == 0:
                with self._input_buffer_1.publish() as publisher:
                    publisher.publish_complete(error=RuntimeError("Test"), timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            else:
                self.fail("Runnable should have stopped")

        self._run_runnable(actions)

        self._output_subscriber_1.assert_completed(True, self)
        self._output_subscriber_2.assert_completed(True, self)
        self._output_subscriber_1.assert_error_values(["RuntimeError('Test')"], self)
        self._output_subscriber_2.assert_error_values(["RuntimeError('Test')"], self)

    def test_error_when_handling_on_complete_passed_out(self) -> None:
        # If we have an exception when handling on_complete we should try to send the exception to any uncompleted subscribers, then end

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter

            if count == 0:
                with self._input_buffer_2.publish() as publisher:
                    publisher.publish_complete(error=None, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            else:
                self.fail("Runnable should have stopped")

        self._output_subscriber_2.error_in_on_complete = 1  # raise an error the first time but succeed the second time (recording the error)
        self._run_runnable(actions)

        self._output_subscriber_1.assert_completed(True, self)
        self._output_subscriber_2.assert_completed(True, self)
        self._output_subscriber_3.assert_completed(True, self)
        self._output_subscriber_1.assert_error_values(["RuntimeError('Error in on_complete 1')"], self)
        self._output_subscriber_2.assert_error_values(["RuntimeError('Error in on_complete 1')"], self)
        self._output_subscriber_3.assert_error_values(["RuntimeError('Error in on_complete 1')"], self)

    def test_persistent_error_when_handling_on_complete_passed_out(self) -> None:
        # If we have an exception when handling on_complete we should try to send the exception to any uncompleted subscribers, then end;
        # if on_complete errors again, we just give up on that subscriber but try the others

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter

            if count == 0:
                with self._input_buffer_2.publish() as publisher:
                    publisher.publish_complete(error=None, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            else:
                self.fail("Runnable should have stopped")

        self._output_subscriber_2.error_in_on_complete = 2
        self._run_runnable(actions)

        self._output_subscriber_1.assert_completed(True, self)
        self._output_subscriber_2.assert_completed(False, self)  # couldn't get the error out on this channel, it kept raising errors
        self._output_subscriber_3.assert_completed(True, self)
        self._output_subscriber_1.assert_error_values(["RuntimeError('Error in on_complete 1')"], self)
        self._output_subscriber_2.assert_error_values([], self)
        self._output_subscriber_3.assert_error_values(["RuntimeError('Error in on_complete 1')"], self)

    def test_persistent_error_reraised_when_handling_on_complete_passed_out(self) -> None:
        # As above but the subscriber re-raises the error it is given

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter

            if count == 0:
                with self._input_buffer_2.publish() as publisher:
                    publisher.publish_complete(error=None, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            else:
                self.fail("Runnable should have stopped")

        self._output_subscriber_2.error_in_on_complete = 2
        self._output_subscriber_2.error_in_on_complete_reraise = True
        self._run_runnable(actions)

        self._output_subscriber_1.assert_completed(True, self)
        self._output_subscriber_2.assert_completed(False, self)  # couldn't get the error out on this channel, it kept raising errors
        self._output_subscriber_3.assert_completed(True, self)
        self._output_subscriber_1.assert_error_values(["RuntimeError('Error in on_complete 1')"], self)
        self._output_subscriber_2.assert_error_values([], self)
        self._output_subscriber_3.assert_error_values(["RuntimeError('Error in on_complete 1')"], self)

    def test_error_handling_on_complete_cannot_be_passed_out_hook_handles(self) -> None:
        # If we have an exception and we can't manage to send it on any output, then it gets passed to the execution_ending_hook; if that handles it,
        # the runnable should end without error

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter

            if count == 0:
                with self._input_buffer_2.publish() as publisher:
                    publisher.publish_complete(error=None, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            else:
                self.fail("Runnable should have stopped")

        self._output_subscriber_1.error_in_on_complete = 2
        self._output_subscriber_2.error_in_on_complete = 2
        self._output_subscriber_3.error_in_on_complete = 2
        self._run_runnable(actions, immortal=True, handle_in_ending_hook=True)  # no exceptions

    def test_error_handling_on_complete_cannot_be_passed_out_hook_does_not_handle(self) -> None:
        # If we have an exception and we can't manage to send it on any output, then it gets passed to the execution_ending_hook; if that doesn't handle it,
        # the runnable should raise an error

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter

            if count == 0:
                with self._input_buffer_2.publish() as publisher:
                    publisher.publish_complete(error=None, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            else:
                self.fail("Runnable should have stopped")

        self._output_subscriber_1.error_in_on_complete = 2
        self._output_subscriber_2.error_in_on_complete = 2
        self._output_subscriber_3.error_in_on_complete = 2

        with self.assertRaisesRegex(RuntimeError, "Error in on_complete 1"):
            self._run_runnable(actions, immortal=True, handle_in_ending_hook=False)

    # as above 3 but with incoming error - new error raised
    # as above 3 but with incoming error - error re-raised

    @assert_no_warnings_or_errors_logged
    def test_values_passed_on(self) -> None:
        # Tests that inputs flow to outputs.

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter

            if count == 0:
                with self._input_buffer_1.publish() as publisher:
                    publisher.publish_value("1")
                    publisher.publish_value("2")
                    publisher.publish_complete(error=None)
            elif count == 1:
                with self._input_buffer_2.publish() as publisher:
                    publisher.publish_value("3")
                    publisher.publish_value("4")
                    publisher.publish_complete(error=None)
            else:
                self.fail("Runnable should have stopped")

        runnable = self._run_runnable(actions)

        self.assertEqual(2, runnable.call_count)
        self._output_subscriber_1.assert_published_values(["1", "2"], self)
        self._output_subscriber_2.assert_published_values(["3", "4"], self)
        self._output_subscriber_1.assert_completed(True, self)
        self._output_subscriber_2.assert_completed(True, self)
        self._output_subscriber_1.assert_error_values([], self)
        self._output_subscriber_2.assert_error_values([], self)

    @assert_no_warnings_or_errors_logged
    def test_known_command_handled(self) -> None:
        # The runnable should handle a command that it understands

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            if count == 0:
                the_runnable.send_command(AcceptedStatusCommand())
            elif count == 1:
                the_runnable.stop()
            else:
                self.fail("Runnable should have stopped")

        runnable = self._run_runnable(actions)

        self.assertTrue(runnable.special_command_received)
        self._output_subscriber_1.assert_completed(True, self)
        self._output_subscriber_2.assert_completed(True, self)
        self._output_subscriber_1.assert_error_values([], self)
        self._output_subscriber_2.assert_error_values([], self)

    @assert_no_warnings_or_errors_logged
    def test_unknown_command_causes_error(self) -> None:
        # The runnable should produce an error if it receives a command that it doesn't understand

        class DerivedCommandMessage(CommandMessage):
            def __str__(self) -> str:
                return 'Derived Command message'

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            if count == 0:
                the_runnable.send_command(DerivedCommandMessage())
            else:
                self.fail("Runnable should have stopped")

        self._run_runnable(actions)

        self._output_subscriber_1.assert_completed(True, self)
        self._output_subscriber_2.assert_completed(True, self)
        self._output_subscriber_1.assert_error_values(["RuntimeError('Could not handle unknown command: Derived Command message')"], self)
        self._output_subscriber_2.assert_error_values(["RuntimeError('Could not handle unknown command: Derived Command message')"], self)

    @assert_no_warnings_or_errors_logged
    def test_giving_same_subscriber_for_multiple_observables(self) -> None:
        # It is legal to add multiple input buffers that reference the same output subscription. That subscription should receive the vallues from all inputs,
        # and should only be completed when all its input buffers have completed.

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter

            if count == 0:
                with self._input_buffer_1.publish() as publisher:
                    publisher.publish_value("1")
                    publisher.publish_complete(error=None)
            elif count == 1:
                with self._input_buffer_2.publish() as publisher:
                    publisher.publish_value("2")
                    publisher.publish_complete(error=None)
            else:
                self.fail("Runnable should have stopped")

        runnable = self._run_runnable(actions, wire_both_inputs_to_one_output=True)

        self.assertEqual(2, runnable.call_count)
        self._output_subscriber_1.assert_published_values(["1", "2"], self)  # subscriber 1 receives both inputs
        self._output_subscriber_1.assert_error_values([], self)
        self._output_subscriber_2.assert_published_values([], self)  # subscriber 2 not used at all because never given to the runnable
        self._output_subscriber_2.assert_completed(False, self)

    @assert_no_warnings_or_errors_logged
    def test_event_is_waited_upon_raised_by_input_buffers(self) -> None:
        # The runnable should wake up and handle input values. It should sleep while waiting for inputs or commands.

        def publish_after_delay(input_buffer: Publishable[str]) -> None:
            time.sleep(DELAY)
            with input_buffer.publish() as publisher:
                publisher.publish_value("1")

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            if count == 0:
                self._thread = Thread(name="publish after delay", target=publish_after_delay, args=(self._input_buffer_1,))
                self._thread.start()
            elif count == 1:
                self._thread = Thread(name="publish after delay", target=publish_after_delay, args=(self._input_buffer_2,))
                self._thread.start()
            elif count == 2:
                the_runnable.stop()
            else:
                self.fail("Runnable should have stopped")

        runnable = self._run_runnable(actions)
        if self._thread:
            self._thread.join()

        for delays in range(2):
            time_waiting = runnable.loop_times[delays + 1] - runnable.loop_times[delays]
            self.assertGreater(time_waiting, DELAY - TIME_TOLERANCE, f"{delays}: Runnable should have waited on its event variable for at least {DELAY}, took {time_waiting}")
            self.assertLess(time_waiting, DELAY + TIME_TOLERANCE, f"{delays}: Runnable should have waited on its event variable for only {DELAY}, took {time_waiting}")

    @assert_no_warnings_or_errors_logged
    def test_event_is_waited_upon_raised_by_command_buffer(self) -> None:
        # The runnable should wake up and handle commands. It should sleep while waiting for inputs or commands.

        def send_command_after_delay(the_runnable: TestMultiBufferServicingRunnable) -> None:
            time.sleep(DELAY)
            the_runnable.send_command(AcceptedStatusCommand())

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            if count < 3:
                self._thread = Thread(name="send command after delay", target=send_command_after_delay, args=(the_runnable,))
                self._thread.start()
            elif count == 3:
                the_runnable.stop()
            else:
                self.fail("Runnable should have stopped")

        runnable = self._run_runnable(actions)
        if self._thread:
            self._thread.join()

        for delays in range(3):
            time_waiting = runnable.loop_times[delays + 1] - runnable.loop_times[delays]
            self.assertGreater(time_waiting, DELAY - TIME_TOLERANCE, f"{delays}: Runnable should have waited on its event variable for at least {DELAY}, took {time_waiting}")
            self.assertLess(time_waiting, DELAY + TIME_TOLERANCE, f"{delays}: Runnable should have waited on its event variable for only {DELAY}, took {time_waiting}")

    @assert_no_warnings_or_errors_logged
    def test_must_add_subscription_before_executing(self) -> None:
        # At least on subscription must be added before the runner is started.

        def dummy_test_callable(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter
            del count  # Unused parameter
            self.fail("Runnable should not have executed")

        with TestInlineBuffer[CommandMessage](10, "Test Command buffer") as command_buffer, \
                TestInlineBuffer[StatusMessage](10, "Test status buffer") as wrapped_status_buffer:
            runnable = TestMultiBufferServicingRunnable(dummy_test_callable, [])

            status_buffer = StatusBuffer(wrapped_status_buffer)
            with status_buffer.publish() as status_publisher, status_buffer.subscribe() as status_subscription:
                runnable.runner_accessor.set_command_buffer(command_buffer)
                runnable.runner_accessor.set_status_buffer_publisher(status_publisher)
                runnable.runner_accessor.set_status_buffer_subscription(status_subscription)
                # runnable.add_subscription not called
                with self.assertRaisesRegex(RuntimeError, "At least one subscription must be added before executing"):
                    runnable.runner_accessor.run_execute()

    @assert_no_warnings_or_errors_logged
    def test_cannot_subscribe_the_same_observable(self) -> None:
        # It is not legal to subscribe the same input buffer more than once.

        def dummy_test_callable(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter
            del count  # Unused parameter
            self.fail("Runnable should not have executed")

        runnable = TestMultiBufferServicingRunnable(dummy_test_callable, [])
        runnable.add_subscription(self._input_buffer_2, self._output_subscriber_2)
        with self.assertRaisesRegex(RuntimeError, "Observable is already present"):
            runnable.add_subscription(self._input_buffer_2, self._output_subscriber_2)
        runnable.remove_subscription(self._input_buffer_2, self._output_subscriber_2)
        runnable.add_subscription(self._input_buffer_2, self._output_subscriber_2)

    @assert_no_warnings_or_errors_logged
    def test_must_set_command_buffer_before_executing(self) -> None:
        # The command buffer must be set before the runnable is started. (In practice this will be done by the runner).

        def dummy_test_callable(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter
            del count  # Unused parameter
            self.fail("Runnable should not have executed")

        with TestInlineBuffer[StatusMessage](10, "Test status buffer") as wrapped_status_buffer:
            runnable = TestMultiBufferServicingRunnable(dummy_test_callable, [])
            runnable.add_subscription(self._input_buffer_2, self._output_subscriber_2)
            status_buffer = StatusBuffer(wrapped_status_buffer)
            with status_buffer.publish() as status_publisher, status_buffer.subscribe() as status_subscription:
                # runnable.runner_accessor.set_command_buffer not called
                runnable.runner_accessor.set_status_buffer_publisher(status_publisher)
                runnable.runner_accessor.set_status_buffer_subscription(status_subscription)
                with self.assertRaisesRegex(RuntimeError, "set_command_buffer has not been called"):
                    runnable.runner_accessor.run_execute()

    @assert_no_warnings_or_errors_logged
    def test_must_set_status_buffer_publisher_before_executing(self) -> None:
        # The status buffer must be set before the runnable is started. (In practice this will be done by the runner).

        def dummy_test_callable(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter
            del count  # Unused parameter
            self.fail("Runnable should not have executed")

        with TestInlineBuffer[CommandMessage](10, "Test Command buffer") as command_buffer, TestInlineBuffer[StatusMessage](10, "Test status buffer") as wrapped_status_buffer:
            runnable = TestMultiBufferServicingRunnable(dummy_test_callable, [])
            runnable.add_subscription(self._input_buffer_2, self._output_subscriber_2)
            status_buffer = StatusBuffer(wrapped_status_buffer)
            with status_buffer.subscribe() as status_subscription:
                runnable.runner_accessor.set_command_buffer(command_buffer)
                # runnable.runner_accessor.set_status_buffer_publisher not called
                runnable.runner_accessor.set_status_buffer_subscription(status_subscription)
                with self.assertRaisesRegex(RuntimeError, "set_status_buffer_publisher has not been called"):
                    runnable.runner_accessor.run_execute()

    @assert_no_warnings_or_errors_logged
    def test_must_set_status_buffer_subscription_before_waiting_for_message(self) -> None:
        # The status buffer must be set before the runnable is started. (In practice this will be done by the runner).

        def dummy_test_callable(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter
            del count  # Unused parameter
            self.fail("Runnable should not have executed")

        with TestInlineBuffer[CommandMessage](10, "Test Command buffer") as command_buffer, TestInlineBuffer[StatusMessage](10, "Test status buffer") as wrapped_status_buffer:
            runnable = TestMultiBufferServicingRunnable(dummy_test_callable, [])
            runnable.add_subscription(self._input_buffer_2, self._output_subscriber_2)
            status_buffer = StatusBuffer(wrapped_status_buffer)
            with status_buffer.publish() as status_publisher:
                runnable.runner_accessor.set_command_buffer(command_buffer)
                runnable.runner_accessor.set_status_buffer_publisher(status_publisher)
                # runnable.runner_accessor.set_status_buffer_subscription not called
                runnable.runner_accessor.run_execute()
                with self.assertRaisesRegex(RuntimeError, "set_status_buffer_subscription has not been called"):
                    runnable.wait_for_status_message(StartedStatusMessage, 0.0)

    @assert_no_warnings_or_errors_logged
    def test_cant_subscribe_while_executing(self) -> None:
        # It is illegal to subscribe once the runnable is executing

        def actions(the_runnable: TestMultiBufferServicingRunnable, count: int) -> None:
            if count == 0:
                with self.assertRaisesRegex(RuntimeError, "Can't add a subscription while the runnable is executing"):
                    the_runnable.add_subscription(self._input_buffer_1, self._output_subscriber_1)

                # must do something to raise the runnable's event variable, or it will deadlock
                the_runnable.stop()
            else:
                self.fail("Runnable should have stopped")

        self._run_runnable(actions)

        self._output_subscriber_1.assert_completed(True, self)
        self._output_subscriber_2.assert_completed(True, self)
        self._output_subscriber_1.assert_error_values([], self)
        self._output_subscriber_2.assert_error_values([], self)

    # noinspection PyTypeChecker
    @no_type_check
    def test_illegal_params(self) -> None:
        with self.assertRaises(ValueError):
            MultiBufferServicingRunnable(None, [])
        with self.assertRaises(ValueError):
            MultiBufferServicingRunnable("name", None)
        with self.assertRaises(ValueError):
            MultiBufferServicingRunnable("name", [], tick_interval=0)
        with self.assertRaises(ValueError):
            MultiBufferServicingRunnable("name", [], tick_interval=-1.0)
        runnable = MultiBufferServicingRunnable("name", [])
        with self.assertRaises(ValueError):
            runnable._add_subscription(None, TestSubscriber())
        with self.assertRaises(ValueError):
            runnable._add_subscription(TestInlineBuffer(1, "name"), None)

        def runnable_factory():
            return IllegalParamsTestRunnable("name", [])

        call_runnable_method_on_running_instance(self, runnable_factory, lambda r: r.set_tick_interval(None))
        call_runnable_method_on_running_instance(self, runnable_factory, lambda r: r.set_tick_interval(0.0))
        call_runnable_method_on_running_instance(self, runnable_factory, lambda r: r.set_tick_interval(-1))

    # The regular ticking feature of MultiBufferServicingRunnable is tested in the "slow tests"

    def _run_runnable(self,
                      test_callable: Callable[[TestMultiBufferServicingRunnable, int], None],
                      *,
                      wire_both_inputs_to_one_output: bool = False,
                      immortal: bool = False,
                      handle_in_ending_hook: bool = False
                      ) -> TestMultiBufferServicingRunnable:
        with TestInlineBuffer[CommandMessage](10, "Test Command buffer") as command_buffer, \
                TestInlineBuffer[StatusMessage](10, "Test status buffer") as wrapped_status_buffer:
            runnable = TestMultiBufferServicingRunnable(test_callable, [], immortal=immortal, handle_in_ending_hook=handle_in_ending_hook)
            runnable.add_subscription(self._input_buffer_1, self._output_subscriber_1)
            if wire_both_inputs_to_one_output:
                runnable.add_subscription(self._input_buffer_2, self._output_subscriber_1)
            else:
                runnable.add_subscription(self._input_buffer_2, self._output_subscriber_2)
            runnable.add_subscription(self._input_buffer_3, self._output_subscriber_3)
            status_buffer = StatusBuffer(wrapped_status_buffer)
            with status_buffer.publish() as status_publisher, status_buffer.subscribe() as status_subscription:
                runnable.runner_accessor.set_command_buffer(command_buffer)
                runnable.runner_accessor.set_status_buffer_publisher(status_publisher)
                runnable.runner_accessor.set_status_buffer_subscription(status_subscription)
                runnable.runner_accessor.set_command_publisher(command_buffer.publish())
                runnable.runner_accessor.run_execute()
            return runnable

    def _run_runnable_in_thread(self,
                                test_callable: Callable[[TestMultiBufferServicingRunnable, int], None],
                                *,
                                wire_both_inputs_to_one_output: bool = False) -> TestMultiBufferServicingRunnable:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self._run_runnable, test_callable,
                                     wire_both_inputs_to_one_output=wire_both_inputs_to_one_output)
            return future.result()
