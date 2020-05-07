import logging
import queue
import time
from dataclasses import dataclass
from threading import Thread
from typing import Any, Callable, List, Optional, TypeVar, no_type_check
from unittest import TestCase

from puma.attribute import copied
from puma.buffer import Publishable, Publisher
from puma.helpers.testing.logging.decorator import assert_no_warnings_or_errors_logged
from puma.helpers.testing.mixin import NotATestCase
from puma.primitives import AutoResetEvent
from puma.runnable import CommandDrivenRunnable
from puma.runnable._in_runnable_indirect_publisher import _InRunnableIndirectPublisher
from puma.runnable.message import CommandMessage, StartedStatusMessage, StatusBuffer, StatusMessage
from tests.buffer.test_support.buffer_api_test_support import TestSubscriber
from tests.buffer.test_support.test_inline_buffer import TestInlineBuffer
from tests.runnable.test_support.call_runnable_method_on_running_instance import call_runnable_method_on_running_instance

logger = logging.getLogger(__name__)

T = TypeVar("T")

DELAY = 0.5
TIME_TOLERANCE = 0.3


@dataclass(frozen=True)
class AcceptedStatusCommand(CommandMessage):
    """A command that is accepted by TestCommandDrivenRunnable"""


class TestCommandDrivenRunnable(CommandDrivenRunnable, NotATestCase):
    # Test class for CommandDrivenRunnable. Each time round its loop (when a command is received, unless a timeout is specified), _pre_wait_hook gets called
    # and this calls out to a user-supplied test function. This function is given a counter so it can perform a series of test actions.
    _out_publisher: Publisher[str] = copied("_out_publisher")  # Needs to be copy instead of child_only as some tests skip over launching the Runnable
    _test_callable: Callable[['TestCommandDrivenRunnable', int], None] = copied("_test_callable")
    call_count: int = copied("call_count")
    loop_times: List[float] = copied("loop_times")

    def __init__(self,
                 out_buffer: Publishable[str],
                 test_callable: Callable[['TestCommandDrivenRunnable', int], None]) -> None:
        super().__init__("Test runnable", [out_buffer])
        self._out_publisher = self._get_publisher(out_buffer)
        # MyPy complains about assigning to a method: https://github.com/python/mypy/issues/708
        self._test_callable = test_callable  # type: ignore
        self.call_count = 0
        self.loop_times = []

    def send_command(self, command: CommandMessage) -> None:
        self._send_command(command)

    def _pre_wait_hook(self) -> None:
        self.loop_times.append(time.perf_counter())
        # MyPy again complains erroneously: https://github.com/python/mypy/issues/708
        self._test_callable(self, self.call_count)  # type: ignore
        self.call_count += 1

    def _create_in_runnable_indirect_publisher(self, output_buffer: Publishable[Any], publisher_accessor: Callable[[int], Publisher]) -> _InRunnableIndirectPublisher[Any]:
        return TestInRunnableIndirectPublisher(output_buffer, publisher_accessor)

    def _execution_ending_hook(self, error: Optional[Exception]) -> bool:
        self._out_publisher.publish_complete(error)
        return True  # error has been handled

    def _handle_command(self, command: CommandMessage) -> None:
        if isinstance(command, AcceptedStatusCommand):
            self._out_publisher.publish_value("special command received")
        else:
            super()._handle_command(command)


class TestInRunnableIndirectPublisher(_InRunnableIndirectPublisher, NotATestCase):
    _publisher: Optional[Publisher] = copied("_publisher")


class CommandDrivenRunnableTest(TestCase):
    def __init__(self, name: str) -> None:
        super().__init__(name)
        self._output_buffer: TestInlineBuffer[str] = TestInlineBuffer[str](10, "Test output buffer")
        self._thread: Optional[Thread] = None

    def setUp(self) -> None:
        self._output_buffer.__enter__()

    def tearDown(self) -> None:
        self._output_buffer.__exit__(None, None, None)
        # Individual tests that set self._thread should join on it
        self.assertFalse(self._thread and self._thread.is_alive())

    @assert_no_warnings_or_errors_logged
    def test_typical_case(self) -> None:
        # The runnable sends itself a command. When that command is received, it stops
        def actions(the_runnable: TestCommandDrivenRunnable, count: int) -> None:
            if count == 0:
                the_runnable.send_command(AcceptedStatusCommand())
            elif count == 1:
                the_runnable.stop()
            else:
                self.fail("Runnable should have stopped")

        self._run_runnable(actions)

        self._assert_outputs(["special command received"], [])

    @assert_no_warnings_or_errors_logged
    def test_finishes_if_exception_in_runnable(self) -> None:
        # The runnable raises an exception
        def actions(the_runnable: TestCommandDrivenRunnable, count: int) -> None:
            del the_runnable  # Unused parameter

            if count == 0:
                raise RuntimeError("Test error")
            else:
                self.fail("Runnable should have stopped")

        self._run_runnable(actions)

        self._assert_outputs([], ["RuntimeError('Test error')"])

    @assert_no_warnings_or_errors_logged
    def test_unknown_command_causes_error(self) -> None:
        class DerivedCommandMessage(CommandMessage):
            def __str__(self) -> str:
                return 'Derived Command message'

        # The runnable sends itself a message that it doesn't know how to handle
        def actions(the_runnable: TestCommandDrivenRunnable, count: int) -> None:
            if count == 0:
                the_runnable.send_command(DerivedCommandMessage())
            else:
                self.fail("Runnable should have stopped")

        self._run_runnable(actions)

        self._assert_outputs([], ["RuntimeError('Could not handle unknown command: Derived Command message')"])

    @assert_no_warnings_or_errors_logged
    def test_event_is_waited_upon_raised_by_command_buffer(self) -> None:
        # Tests that the runnable does wait on the a command being received (rather than polling)

        def send_command_after_delay(the_runnable: TestCommandDrivenRunnable) -> None:
            time.sleep(DELAY)
            the_runnable.send_command(AcceptedStatusCommand())

        # The runnable sends itself a message after a delay. When that message is received, it stops.
        def actions(the_runnable: TestCommandDrivenRunnable, count: int) -> None:
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
        def dummy_test_callable(the_runnable: TestCommandDrivenRunnable, count: int) -> None:
            del the_runnable  # Unused parameter
            del count  # Unused parameter
            self.fail("Runnable should not have executed")

        with TestInlineBuffer[StatusMessage](10, "Test status buffer") as wrapped_status_buffer:
            runnable = TestCommandDrivenRunnable(self._output_buffer, dummy_test_callable)
            status_buffer = StatusBuffer(wrapped_status_buffer)
            with status_buffer.publish() as status_publisher, status_buffer.subscribe() as status_subscription:
                # runnable.runner_accessor.set_command_buffer not called
                runnable.runner_accessor.set_status_buffer_publisher(status_publisher)
                runnable.runner_accessor.set_status_buffer_subscription(status_subscription)
                with self.assertRaisesRegex(RuntimeError, "set_command_buffer has not been called"):
                    runnable.runner_accessor.run_execute()

    @assert_no_warnings_or_errors_logged
    def test_must_set_status_buffer_publisher_before_executing(self) -> None:
        def dummy_test_callable(the_runnable: TestCommandDrivenRunnable, count: int) -> None:
            del the_runnable  # Unused parameter
            del count  # Unused parameter
            self.fail("Runnable should not have executed")

        with TestInlineBuffer[CommandMessage](10, "Test Command buffer") as command_buffer, TestInlineBuffer[StatusMessage](10, "Test status buffer") as wrapped_status_buffer:
            runnable = TestCommandDrivenRunnable(self._output_buffer, dummy_test_callable)
            status_buffer = StatusBuffer(wrapped_status_buffer)
            with status_buffer.subscribe() as status_subscription:
                runnable.runner_accessor.set_command_buffer(command_buffer)
                # runnable.runner_accessor.set_status_buffer_publisher not called
                runnable.runner_accessor.set_status_buffer_subscription(status_subscription)
                with self.assertRaisesRegex(RuntimeError, "set_status_buffer_publisher has not been called"):
                    runnable.runner_accessor.run_execute()

    @assert_no_warnings_or_errors_logged
    def test_must_set_status_buffer_subscription_before_waiting_for_message(self) -> None:
        def dummy_test_callable(the_runnable: TestCommandDrivenRunnable, count: int) -> None:
            del the_runnable  # Unused parameter
            del count  # Unused parameter
            self.fail("Runnable should not have executed")

        with TestInlineBuffer[CommandMessage](10, "Test Command buffer") as command_buffer, TestInlineBuffer[StatusMessage](10, "Test status buffer") as wrapped_status_buffer:
            runnable = TestCommandDrivenRunnable(self._output_buffer, dummy_test_callable)
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
            CommandDrivenRunnable(None, [])
        with self.assertRaises(ValueError):
            CommandDrivenRunnable("name", None)
        with self.assertRaises(ValueError):
            CommandDrivenRunnable("name", [], tick_interval=0.0)
        with self.assertRaises(ValueError):
            CommandDrivenRunnable("name", [], tick_interval=-1.0)

        def runnable_factory():
            return CommandDrivenRunnable("name", [])

        with self.assertRaises(RuntimeError):
            runnable_factory()._add_subscription(TestInlineBuffer(1, "name"), TestSubscriber())

        call_runnable_method_on_running_instance(self, runnable_factory, lambda r: r.set_tick_interval(None))
        call_runnable_method_on_running_instance(self, runnable_factory, lambda r: r.set_tick_interval(0.0))
        call_runnable_method_on_running_instance(self, runnable_factory, lambda r: r.set_tick_interval(-1))

    def _run_runnable(self,
                      test_callable: Callable[[TestCommandDrivenRunnable, int], None]) -> TestCommandDrivenRunnable:
        with TestInlineBuffer[CommandMessage](10, "Test Command buffer") as command_buffer, \
                TestInlineBuffer[StatusMessage](10, "Test status buffer") as wrapped_status_buffer:
            runnable = TestCommandDrivenRunnable(self._output_buffer, test_callable)
            status_buffer = StatusBuffer(wrapped_status_buffer)
            with status_buffer.publish() as status_publisher, status_buffer.subscribe() as status_subscription:
                runnable.runner_accessor.set_command_buffer(command_buffer)
                runnable.runner_accessor.set_status_buffer_publisher(status_publisher)
                runnable.runner_accessor.set_status_buffer_subscription(status_subscription)
                runnable.runner_accessor.set_command_publisher(command_buffer.publish())
                runnable.runner_accessor.run_execute()
            return runnable

    def _assert_outputs(self, expected_values: List[str], expected_errors: List[str]) -> None:
        subscriber = TestSubscriber()
        event = AutoResetEvent()
        with self._output_buffer.subscribe(event) as subscription:
            while True:
                try:
                    subscription.call_events(subscriber)
                except queue.Empty:
                    break
            subscriber.assert_published_values(expected_values, self)
            subscriber.assert_error_values(expected_errors, self)
            subscriber.assert_completed(True, self)
