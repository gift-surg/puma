from dataclasses import dataclass
from time import sleep
from typing import Optional
from unittest import TestCase

from puma.environment import ThreadEnvironment
from puma.helpers.testing.logging.capture_logs import CaptureLogs
from puma.logging import LogLevel, Logging
from puma.runnable import CommandDrivenRunnable
from puma.runnable.decorator.run_in_child_scope import run_in_child_scope
from puma.runnable.message import CommandMessage, RunInChildScopeCommandMessage
from tests.helpers.timer.test_timer import TestTimer

RUNNABLE_SLEEP_DURATION = 1.1
SLEEP_COMPARE = RUNNABLE_SLEEP_DURATION - 0.1
RATE_CHECK_LESS_THAN_DURATION = 0.1


class RemoteObjectAttributeAndMethodSlowTest(TestCase):

    def test_ensure_warning_is_shown_when_response_is_slow(self) -> None:
        Logging.init_logging()
        environment = ThreadEnvironment()

        with CaptureLogs() as capture:
            runnable = SlowTickRunnable()
            with environment.create_runner(runnable) as runner:
                runner.start_blocking()

                runnable.resume_ticks()
                with TestTimer(self) as runnable_timer:
                    with runnable_timer.sub_timer() as get_object_timer:
                        complex_object = runnable.get_object()
                        get_object_timer.assert_elapsed_time_greater_than(SLEEP_COMPARE)

                    with runnable_timer.sub_timer() as get_complex_attr_timer:
                        complex_attr = complex_object.complex_attr
                        if not complex_attr:
                            self.fail("complex_attr is not set")
                        get_complex_attr_timer.assert_elapsed_time_greater_than(SLEEP_COMPARE)

                    with runnable_timer.sub_timer() as complex_object_method_timer:
                        self.assertEqual(960, complex_attr.object_method(15))
                        complex_object_method_timer.assert_elapsed_time_within_range(SLEEP_COMPARE, 1.2 * SLEEP_COMPARE)

                    with runnable_timer.sub_timer() as complex_object_primitive_attr_timer:
                        self.assertEqual(45, complex_object.primitive_attr)
                        complex_object_primitive_attr_timer.assert_elapsed_time_greater_than(SLEEP_COMPARE)

                    with runnable_timer.sub_timer() as complex_attr_primitive_attr_timer:
                        self.assertEqual(64, complex_attr.primitive_attr)
                        complex_attr_primitive_attr_timer.assert_elapsed_time_greater_than(SLEEP_COMPARE)

                    with runnable_timer.sub_timer() as complex_attr_complex_attr_timer:
                        self.assertEqual(None, complex_attr.complex_attr)
                        complex_attr_complex_attr_timer.assert_elapsed_time_greater_than(SLEEP_COMPARE)

                    # This shouldn't show another warning
                    with runnable_timer.sub_timer() as complex_attr_complex_attr_timer:
                        self.assertEqual(None, complex_attr.complex_attr)
                        complex_attr_complex_attr_timer.assert_elapsed_time_greater_than(SLEEP_COMPARE)

            # Ensure the expected number of warnings were issued
            warning_logs = capture.pop_captured_records().with_levels_in({LogLevel.warn}).containing_message("Slow response when calling").get_lines()
            self.assertEqual(5, len(warning_logs))

            # Check the content
            self.assertTrue("Slow response when calling RemoteObjectAttribute 'complex_attr' - took" in warning_logs[0])
            self.assertTrue("Slow response when calling RemoteObjectMethod 'object_method' - took" in warning_logs[1])
            self.assertTrue("Slow response when calling RemoteObjectAttribute 'primitive_attr' - took" in warning_logs[2])
            self.assertTrue("Slow response when calling RemoteObjectAttribute 'primitive_attr' - took" in warning_logs[3])
            self.assertTrue("Slow response when calling RemoteObjectAttribute 'complex_attr' - took" in warning_logs[4])

    def test_ensure_warning_is_shown_when_rate_is_exceeded(self) -> None:
        Logging.init_logging()
        environment = ThreadEnvironment()

        with CaptureLogs() as capture:
            runnable = RateCheckRunnable()
            with environment.create_runner(runnable) as runner:
                runner.start_blocking()

                with TestTimer(self) as runnable_timer:
                    with runnable_timer.sub_timer() as get_object_timer:
                        complex_object = runnable.get_object()
                        method_call_1 = complex_object.object_method(15)  # noqa: 841
                        method_call_2 = complex_object.object_method(15)  # noqa: 841
                        method_call_3 = complex_object.object_method(15)  # noqa: 841

                        complex_call_1 = complex_object.complex_attr  # noqa: 841
                        complex_call_2 = complex_object.complex_attr  # noqa: 841
                        complex_call_3 = complex_object.complex_attr  # noqa: 841

                        primitive_call_1 = complex_object.primitive_attr  # noqa: 841
                        primitive_call_2 = complex_object.primitive_attr  # noqa: 841
                        primitive_call_3 = complex_object.primitive_attr  # noqa: 841
                        get_object_timer.assert_elapsed_time_less_than(RATE_CHECK_LESS_THAN_DURATION)

                    # Ensure that no warning has been shown
                    warning_logs = capture.pop_captured_records().with_levels_in({LogLevel.warn}).containing_message("Excessive calling of").get_lines()
                    self.assertEqual(0, len(warning_logs))

                    # Call it once more
                    method_call_4 = complex_object.object_method(15)  # noqa: 841
                    complex_call_4 = complex_object.complex_attr  # noqa: 841
                    primitive_call_4 = complex_object.primitive_attr  # noqa: 841
                    warning_logs = capture.pop_captured_records().with_levels_in({LogLevel.warn}).containing_message("Excessive calling of").get_lines()
                    self.assertEqual(3, len(warning_logs))

                    # Check the content
                    self.assertTrue("Excessive calling of RemoteObjectMethod 'object_method' - 4 in less than 1 second(s) (max allowed = 3)" in warning_logs[0])
                    self.assertTrue("Excessive calling of RemoteObjectAttribute 'complex_attr' - 4 in less than 1 second(s) (max allowed = 3)" in warning_logs[1])
                    self.assertTrue("Excessive calling of RemoteObjectAttribute 'primitive_attr' - 4 in less than 1 second(s) (max allowed = 3)" in warning_logs[2])

                    # Ensure that no more warnings are raised
                    method_call_5 = complex_object.object_method(15)  # noqa: 841
                    complex_call_5 = complex_object.complex_attr  # noqa: 841
                    primitive_call_5 = complex_object.primitive_attr  # noqa: 841

                    warning_logs = capture.pop_captured_records().with_levels_in({LogLevel.warn}).containing_message("Excessive calling of").get_lines()
                    self.assertEqual(0, len(warning_logs))


@dataclass(frozen=True)
class SomeObject:
    primitive_attr: int
    complex_attr: Optional["SomeObject"]

    def object_method(self, factor: int) -> int:
        return self.primitive_attr * factor


class RateCheckRunnable(CommandDrivenRunnable):

    def __init__(self) -> None:
        super().__init__("RateCheckRunnable", [])

    @run_in_child_scope
    def get_object(self) -> SomeObject:
        return SomeObject(45, SomeObject(64, None))


class SlowTickRunnable(CommandDrivenRunnable):

    def __init__(self) -> None:
        super().__init__("SlowTickRunnable", [], tick_interval=0.001)

    @run_in_child_scope
    def get_object(self) -> SomeObject:
        return SomeObject(45, SomeObject(64, None))

    def _handle_command(self, command: CommandMessage) -> None:
        """This simulates a slow _on_tick method making remote interactions suffer from high latency"""
        if isinstance(command, RunInChildScopeCommandMessage):
            sleep(RUNNABLE_SLEEP_DURATION)
        super()._handle_command(command)
