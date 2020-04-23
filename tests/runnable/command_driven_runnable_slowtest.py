import queue
import time
from dataclasses import dataclass
from typing import List, Optional
from unittest import TestCase

from puma.attribute import child_only, copied, unmanaged
from puma.buffer import Observable, Publishable, Publisher, Subscriber
from puma.buffer.implementation.managed_queues import ManagedQueueTypes
from puma.primitives import EventType
from puma.runnable import CommandDrivenRunnable, SingleBufferServicingRunnable
from puma.runnable.message import CommandMessage
from puma.runnable.runner import Runner
from tests.environment.parameterisation import EnvironmentTestParameters, environments
from tests.mixin import NotATestCase
from tests.parameterized import parameterized
from tests.test_logging_helpers import assert_no_warnings_or_errors_logged

BUFFER_SIZE = 10
FAIL_TIMEOUT = 10.0
TEST_TIMEOUT = 30.0


# Test CommandDrivenRunnable across threads and processes. Most of the testing of CommandDrivenRunnable is done in a single thread, see
# command_driven_runnable_test.py. This 'slow' test ensures that it really works across threads and processes.


@dataclass(frozen=True)
class StringCommand(CommandMessage):
    string: str


class TestCommandDrivenRunnable(CommandDrivenRunnable, NotATestCase):
    # Test class for CommandDrivenRunnable. Copies a string received in a command to an output buffer.
    _out_publisher: Publisher[str] = child_only("_out_publisher")

    def __init__(self, out_buffer: Publishable[str]) -> None:
        super().__init__("Test runnable", [out_buffer])
        self._out_publisher = self._get_publisher(out_buffer)

    def send_string(self, value: str) -> None:
        self._send_command(StringCommand(value))

    def _handle_command(self, command: CommandMessage) -> None:
        if isinstance(command, StringCommand):
            self._out_publisher.publish_value(command.string)
        else:
            super()._handle_command(command)

    def _execution_ending_hook(self, error: Optional[Exception]) -> bool:
        self._out_publisher.publish_complete(error)
        return True  # Error has been handled


class CapturingSingleSubscriptionRunnable(SingleBufferServicingRunnable[str], Subscriber[str]):
    _completed: EventType = unmanaged("_completed")
    _published_values: ManagedQueueTypes = copied("_published_values")
    _error_values: ManagedQueueTypes = copied("_error_values")

    def __init__(self,
                 observable: Observable[str],
                 completed_event: EventType,
                 published_values: ManagedQueueTypes,
                 error_values: ManagedQueueTypes) -> None:
        name = "Capturing " + observable.buffer_name()
        super().__init__(observable, self, [], name)
        self._completed = completed_event
        self._published_values = published_values
        self._error_values = error_values

    def on_value(self, value: str) -> None:
        self._published_values.put_nowait(value)

    def on_complete(self, error: Optional[BaseException]) -> None:
        if self._completed.is_set():
            raise RuntimeError("Received on_complete more than once")
        self._completed.set()
        if error:
            self._error_values.put_nowait(error)

    def is_completed(self) -> bool:
        return self._completed.is_set()

    def published_values(self) -> List[str]:
        ret = []
        while True:
            try:
                ret.append(self._published_values.get_nowait())
            except queue.Empty:
                break
        return ret

    def error_values(self) -> List[BaseException]:
        ret = []
        while True:
            try:
                ret.append(self._error_values.get_nowait())
            except queue.Empty:
                break
        return ret


class CommandDrivenRunnableSlowTest(TestCase):
    @parameterized(environments)
    @assert_no_warnings_or_errors_logged
    def test_typical_usage(self, param: EnvironmentTestParameters) -> None:
        env = param.environment
        with env.create_buffer(str, BUFFER_SIZE, "Buffer") as out_buffer:
            with env.create_managed_queue(str) as published_values, env.create_managed_queue(Exception) as error_values:
                source_runnable = TestCommandDrivenRunnable(out_buffer)
                sink_runnable = CapturingSingleSubscriptionRunnable(out_buffer, env.create_event(), published_values, error_values)
                with env.create_runner(sink_runnable, "Sink") as sink, env.create_runner(source_runnable) as source:
                    self._run_test(source_runnable, source, sink)
                self._validate_results(sink_runnable)

    def _run_test(self, source_runnable: TestCommandDrivenRunnable, source_runner: Runner, sink_runner: Runner) -> None:
        test_fail_time = time.monotonic() + TEST_TIMEOUT
        source_runner.start()
        sink_runner.start()
        source_runner.wait_until_running(FAIL_TIMEOUT)
        sink_runner.wait_until_running(FAIL_TIMEOUT)

        source_runnable.send_string("Hello")
        source_runnable.send_string("World")
        source_runnable.stop()

        while sink_runner.is_alive():
            sink_runner.check_for_exceptions()
            source_runner.check_for_exceptions()
            time.sleep(0.5)
            self.assertLess(time.monotonic(), test_fail_time, "Deadlock: Test did not end")

    def _validate_results(self, sink_runnable: CapturingSingleSubscriptionRunnable) -> None:
        self.assertTrue(sink_runnable.is_completed())
        self.assertEqual(["Hello", "World"], sink_runnable.published_values())
        self.assertEqual([], sink_runnable.error_values())
