import logging
import time
from concurrent.futures.thread import ThreadPoolExecutor
from dataclasses import dataclass
from threading import Thread
from typing import Callable, List, Optional, TypeVar, no_type_check
from unittest import TestCase

from puma.attribute import copied
from puma.buffer import Observable, Publishable, Subscriber
from puma.helpers.testing.logging.decorator import assert_no_warnings_or_errors_logged
from puma.helpers.testing.mixin import NotATestCase
from puma.runnable import SingleBufferServicingRunnable
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
    """A command that is accepted by TestSingleBufferServicingRunnable"""


class TestSingleBufferServicingRunnable(SingleBufferServicingRunnable, NotATestCase):
    # Test class for CommandDrivenRunnable. Each time round its loop (when a command is received, unless a timeout is specified), _pre_wait_hook gets called
    # and this calls out to a user-supplied test function. This function is given a counter so it can perform a series of test actions.
    _test_callable: Callable[['TestSingleBufferServicingRunnable', int], None] = copied("_test_callable")
    call_count: int = copied("call_count")
    loop_times: List[float] = copied("loop_times")
    special_command_received: bool = copied("special_command_received")

    def __init__(self,
                 observable: Observable[str],
                 subscriber: Subscriber[str],
                 test_callable: Callable[['TestSingleBufferServicingRunnable', int], None]) -> None:
        super().__init__(observable, subscriber, [], "Test runnable")
        # MyPy complains about assigning to a method: https://github.com/python/mypy/issues/708
        self._test_callable = test_callable  # type: ignore
        self.call_count = 0
        self.loop_times = []
        self.special_command_received = False

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


class SingleBufferServicingRunnableTest(TestCase):
    def __init__(self, name: str) -> None:
        super().__init__(name)
        self._input_buffer: TestInlineBuffer[str] = TestInlineBuffer[str](10, "Test input buffer")
        self._output_subscriber: TestSubscriber = TestSubscriber()
        self._thread: Optional[Thread] = None

    def setUp(self) -> None:
        self._input_buffer.__enter__()

    def tearDown(self) -> None:
        self._input_buffer.__exit__(None, None, None)
        # Individual tests that set self._thread should join on it
        self.assertFalse(self._thread and self._thread.is_alive())

    @assert_no_warnings_or_errors_logged
    def test_finishes_if_commanded_to_stop(self) -> None:
        # The runnable must stop when told to stop

        def actions(the_runnable: TestSingleBufferServicingRunnable, count: int) -> None:
            if count < 100:
                with self._input_buffer.publish() as publisher:
                    publisher.publish_value(str(count))
            elif count == 100:
                the_runnable.stop()
            else:
                self.fail("Runnable should have stopped")

        self._run_runnable(actions)

        self._output_subscriber.assert_published_values([str(i) for i in range(100)], self)
        self._output_subscriber.assert_completed(True, self)
        self._output_subscriber.assert_error_values([], self)

    @assert_no_warnings_or_errors_logged
    def test_in_thread_finishes_if_commanded_to_stop(self) -> None:
        # same as previous test but the runner is in a separate thread

        def actions(the_runnable: TestSingleBufferServicingRunnable, count: int) -> None:
            if count < 100:
                with self._input_buffer.publish() as publisher:
                    publisher.publish_value(str(count))
            elif count == 100:
                the_runnable.stop()
            else:
                self.fail("Runnable should have stopped")

        self._run_runnable_in_thread(actions)

        self._output_subscriber.assert_published_values([str(i) for i in range(100)], self)
        self._output_subscriber.assert_completed(True, self)
        self._output_subscriber.assert_error_values([], self)

    @assert_no_warnings_or_errors_logged
    def test_finishes_when_observable_finished(self) -> None:
        # The runnable must stop when its input buffer has sent complete

        def actions(the_runnable: TestSingleBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter

            # Buffer counts to 50 then finishes
            if count < 50:
                with self._input_buffer.publish() as publisher:
                    publisher.publish_value(str(count))
            elif count == 50:
                with self._input_buffer.publish() as publisher:
                    publisher.publish_complete(error=None, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            else:
                self.fail("Runnable should have stopped")

        runnable = self._run_runnable(actions)

        self.assertEqual(51, runnable.call_count)  # verifies that it didn't stop until it had received on_complete on both inputs
        self._output_subscriber.assert_published_values([str(i) for i in range(50)], self)
        self._output_subscriber.assert_completed(True, self)
        self._output_subscriber.assert_error_values([], self)

    @assert_no_warnings_or_errors_logged
    def test_in_thread_finishes_when_all_observables_finished(self) -> None:
        # same as previous test but the runner is in a separate thread

        def actions(the_runnable: TestSingleBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter

            # Buffer counts to 50 then finishes
            if count < 50:
                with self._input_buffer.publish() as publisher:
                    publisher.publish_value(str(count))
            elif count == 50:
                with self._input_buffer.publish() as publisher:
                    publisher.publish_complete(error=None, timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            else:
                self.fail("Runnable should have stopped")

        runnable = self._run_runnable_in_thread(actions)

        self.assertEqual(51, runnable.call_count)  # verifies that it didn't stop until it had received on_complete on both inputs
        self._output_subscriber.assert_published_values([str(i) for i in range(50)], self)
        self._output_subscriber.assert_completed(True, self)
        self._output_subscriber.assert_error_values([], self)

    @assert_no_warnings_or_errors_logged
    def test_finishes_if_exception_in_runnable(self) -> None:
        # If an error occurs in the runnable then it should be sent out in an completed message

        def actions(the_runnable: TestSingleBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter

            if count == 0:
                raise RuntimeError("Test error")
            else:
                self.fail("Runnable should have stopped")

        self._run_runnable(actions)

        self._output_subscriber.assert_completed(True, self)
        self._output_subscriber.assert_error_values(["RuntimeError('Test error')"], self)

    @assert_no_warnings_or_errors_logged
    def test_received_error_is_passed_on(self) -> None:
        # If we receive an error in publish_complete on the input buffer, we pass that error on to the subscriber and stop.

        def actions(the_runnable: TestSingleBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter

            if count == 0:
                with self._input_buffer.publish() as publisher:
                    publisher.publish_complete(error=RuntimeError("Test"), timeout=TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            else:
                self.fail("Runnable should have stopped")

        runnable = self._run_runnable(actions)

        self.assertEqual(1, runnable.call_count)
        self._output_subscriber.assert_completed(True, self)
        self._output_subscriber.assert_error_values(["RuntimeError('Test')"], self)

    @assert_no_warnings_or_errors_logged
    def test_values_passed_on(self) -> None:
        # Tests that inputs flow to outputs.

        def actions(the_runnable: TestSingleBufferServicingRunnable, count: int) -> None:
            if count == 0:
                with self._input_buffer.publish() as publisher:
                    publisher.publish_value("1")
                with self._input_buffer.publish() as publisher:
                    publisher.publish_value("2")
            elif count == 1:
                the_runnable.stop()
            else:
                self.fail("Runnable should have stopped")

        runnable = self._run_runnable(actions)

        self.assertEqual(2, runnable.call_count)
        self._output_subscriber.assert_published_values(["1", "2"], self)
        self._output_subscriber.assert_completed(True, self)
        self._output_subscriber.assert_error_values([], self)

    @assert_no_warnings_or_errors_logged
    def test_known_command_handled(self) -> None:
        # The runnable should handle a command that it understands

        def actions(the_runnable: TestSingleBufferServicingRunnable, count: int) -> None:
            if count == 0:
                the_runnable.send_command(AcceptedStatusCommand())
            elif count == 1:
                the_runnable.stop()
            else:
                self.fail("Runnable should have stopped")

        runnable = self._run_runnable(actions)

        self.assertTrue(runnable.special_command_received)
        self._output_subscriber.assert_completed(True, self)
        self._output_subscriber.assert_error_values([], self)

    @assert_no_warnings_or_errors_logged
    def test_unknown_command_causes_error(self) -> None:
        # The runnable should produce an error if it receives a command that it doesn't understand

        class DerivedCommandMessage(CommandMessage):
            def __str__(self) -> str:
                return 'Derived Command message'

        def actions(the_runnable: TestSingleBufferServicingRunnable, count: int) -> None:
            if count == 0:
                the_runnable.send_command(DerivedCommandMessage())
            else:
                self.fail("Runnable should have stopped")

        self._run_runnable(actions)

        self._output_subscriber.assert_completed(True, self)
        self._output_subscriber.assert_error_values(["RuntimeError('Could not handle unknown command: Derived Command message')"], self)

    @assert_no_warnings_or_errors_logged
    def test_event_is_waited_upon_raised_by_input_buffer(self) -> None:
        # The runnable should wake up and handle input values. It should sleep while waiting for inputs or commands.

        def publish_after_delay(input_buffer: Publishable[str]) -> None:
            time.sleep(DELAY)
            with input_buffer.publish() as publisher:
                publisher.publish_value("1")

        def actions(the_runnable: TestSingleBufferServicingRunnable, count: int) -> None:
            if count == 0:
                self._thread = Thread(name="publish after delay", target=publish_after_delay, args=(self._input_buffer,))
                self._thread.start()
            elif count == 1:
                the_runnable.stop()
            else:
                self.fail("Runnable should have stopped")

        runnable = self._run_runnable(actions)
        if self._thread:
            self._thread.join()

        time_waiting = runnable.loop_times[1] - runnable.loop_times[0]
        self.assertGreater(time_waiting, DELAY - TIME_TOLERANCE, f"Runnable should have waited on its event variable for at least {DELAY}, took {time_waiting}")
        self.assertLess(time_waiting, DELAY + TIME_TOLERANCE, f"Runnable should have waited on its event variable for only {DELAY}, took {time_waiting}")

    @assert_no_warnings_or_errors_logged
    def test_event_is_waited_upon_raised_by_command_buffer(self) -> None:
        # The runnable should wake up and handle commands. It should sleep while waiting for inputs or commands.

        def send_command_after_delay(the_runnable: TestSingleBufferServicingRunnable) -> None:
            time.sleep(DELAY)
            the_runnable.send_command(AcceptedStatusCommand())

        def actions(the_runnable: TestSingleBufferServicingRunnable, count: int) -> None:
            if count == 0:
                self._thread = Thread(name="send command after delay", target=send_command_after_delay, args=(the_runnable,))
                self._thread.start()
            elif count == 1:
                the_runnable.stop()
            else:
                self.fail("Runnable should have stopped")

        runnable = self._run_runnable(actions)
        if self._thread:
            self._thread.join()

        time_waiting = runnable.loop_times[1] - runnable.loop_times[0]
        self.assertGreater(time_waiting, DELAY - TIME_TOLERANCE, f"Runnable should have waited on its event variable for at least {DELAY}, took {time_waiting}")
        self.assertLess(time_waiting, DELAY + TIME_TOLERANCE, f"Runnable should have waited on its event variable for only {DELAY}, took {time_waiting}")

    @assert_no_warnings_or_errors_logged
    def test_must_set_command_buffer_before_executing(self) -> None:
        # The command buffer must be set before the runnable is started. (In practice this will be done by the runner).

        def dummy_test_callable(the_runnable: TestSingleBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter
            del count  # Unused parameter
            self.fail("Runnable should not have executed")

        with TestInlineBuffer[StatusMessage](10, "Test status buffer") as wrapped_status_buffer:
            runnable = TestSingleBufferServicingRunnable(self._input_buffer, self._output_subscriber, dummy_test_callable)
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

        def dummy_test_callable(the_runnable: TestSingleBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter
            del count  # Unused parameter
            self.fail("Runnable should not have executed")

        with TestInlineBuffer[CommandMessage](10, "Test Command buffer") as command_buffer, TestInlineBuffer[StatusMessage](10, "Test status buffer") as wrapped_status_buffer:
            runnable = TestSingleBufferServicingRunnable(self._input_buffer, self._output_subscriber, dummy_test_callable)
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

        def dummy_test_callable(the_runnable: TestSingleBufferServicingRunnable, count: int) -> None:
            del the_runnable  # Unused parameter
            del count  # Unused parameter
            self.fail("Runnable should not have executed")

        with TestInlineBuffer[CommandMessage](10, "Test Command buffer") as command_buffer, TestInlineBuffer[StatusMessage](10, "Test status buffer") as wrapped_status_buffer:
            runnable = TestSingleBufferServicingRunnable(self._input_buffer, self._output_subscriber, dummy_test_callable)
            status_buffer = StatusBuffer(wrapped_status_buffer)
            with status_buffer.publish() as status_publisher:
                runnable.runner_accessor.set_command_buffer(command_buffer)
                runnable.runner_accessor.set_status_buffer_publisher(status_publisher)
                # runnable.runner_accessor.set_status_buffer_subscription not called
                runnable.runner_accessor.run_execute()
                with self.assertRaisesRegex(RuntimeError, "set_status_buffer_subscription has not been called"):
                    runnable.wait_for_status_message(StartedStatusMessage, 0.0)

    # noinspection PyTypeChecker
    @no_type_check
    def test_illegal_params(self) -> None:
        with self.assertRaises(ValueError):
            SingleBufferServicingRunnable(None, self._output_subscriber, [], "name")
        with self.assertRaises(ValueError):
            SingleBufferServicingRunnable(self._input_buffer, None, [], "name")
        with self.assertRaises(ValueError):
            SingleBufferServicingRunnable(self._input_buffer, self._output_subscriber, None, "name")
        with self.assertRaises(ValueError):
            SingleBufferServicingRunnable(self._input_buffer, self._output_subscriber, [], None)
        with self.assertRaises(ValueError):
            SingleBufferServicingRunnable(self._input_buffer, self._output_subscriber, [], "name", tick_interval=0.0)
        with self.assertRaises(ValueError):
            SingleBufferServicingRunnable(self._input_buffer, self._output_subscriber, [], "name", tick_interval=-1.0)

        def runnable_factory():
            return SingleBufferServicingRunnable(self._input_buffer, self._output_subscriber, [], "name")

        call_runnable_method_on_running_instance(self, runnable_factory, lambda r: r.set_tick_interval(None))
        call_runnable_method_on_running_instance(self, runnable_factory, lambda r: r.set_tick_interval(0.0))
        call_runnable_method_on_running_instance(self, runnable_factory, lambda r: r.set_tick_interval(-1))

    def _run_runnable(self,
                      test_callable: Callable[[TestSingleBufferServicingRunnable, int], None]) -> TestSingleBufferServicingRunnable:
        with TestInlineBuffer[CommandMessage](10, "Test Command buffer") as command_buffer, \
                TestInlineBuffer[StatusMessage](10, "Test status buffer") as wrapped_status_buffer:
            runnable = TestSingleBufferServicingRunnable(self._input_buffer, self._output_subscriber, test_callable)
            status_buffer = StatusBuffer(wrapped_status_buffer)
            with status_buffer.publish() as status_publisher, status_buffer.subscribe() as status_subscription:
                runnable.runner_accessor.set_command_buffer(command_buffer)
                runnable.runner_accessor.set_status_buffer_publisher(status_publisher)
                runnable.runner_accessor.set_status_buffer_subscription(status_subscription)
                runnable.runner_accessor.set_command_publisher(command_buffer.publish())
                runnable.runner_accessor.run_execute()
            return runnable

    def _run_runnable_in_thread(self,
                                test_callable: Callable[[TestSingleBufferServicingRunnable, int], None]) -> TestSingleBufferServicingRunnable:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(self._run_runnable, test_callable)
            return future.result()
