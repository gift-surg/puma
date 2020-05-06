import logging
from typing import no_type_check
from unittest import TestCase

from puma.context import MustBeContextManagedError
from puma.helpers.string import list_str
from puma.helpers.testing.logging.decorator import assert_no_warnings_or_errors_logged
from puma.runnable.runner import ProcessRunner, ThreadRunner
from puma.runnable.runner.runner import RunnerStillAliveError
from tests.runnable.runner.test_inline_runnable import TestInlineRunnable
from tests.runnable.runner.test_inline_runner import TST_BUFFER_SIZES, TestInlineRunner

logger = logging.getLogger(__name__)


class RunnerTest(TestCase):

    def setUp(self) -> None:
        self._runnable: TestInlineRunnable = TestInlineRunnable("Test")

    @assert_no_warnings_or_errors_logged
    def test_name_supplied(self) -> None:
        with TestInlineRunner(self._runnable, "abuff_name") as runner:
            self.assertEqual("abuff_name", runner.get_name())

    @assert_no_warnings_or_errors_logged
    def test_name_not_supplied(self) -> None:
        with TestInlineRunner(self._runnable, None) as runner:
            self.assertEqual("TestInlineRunner of Test", runner.get_name())

    @assert_no_warnings_or_errors_logged
    def test_is_initialised(self) -> None:
        runner = TestInlineRunner(self._runnable, None)
        with self.assertRaisesRegex(MustBeContextManagedError, "Must be context managed"):
            runner.start()
        with self.assertRaisesRegex(MustBeContextManagedError, "Must be context managed"):
            runner.join()

    @assert_no_warnings_or_errors_logged
    def test_start_stop_join(self) -> None:
        with TestInlineRunner(self._runnable, None) as runner:
            self.assertFalse(runner.started)
            self.assertFalse(runner.joined)
            self.assertFalse(runner.is_alive())
            self.assertFalse(self._runnable.executed)
            self.assertFalse(self._runnable.stopped)

            runner.start()

            self.assertTrue(runner.started)
            self.assertFalse(runner.joined)
            self.assertTrue(runner.is_alive())
            self.assertTrue(self._runnable.executed)
            self.assertFalse(self._runnable.stopped)

            runner.stop()

            self.assertTrue(runner.started)
            self.assertFalse(runner.joined)
            self.assertFalse(runner.is_alive())
            self.assertTrue(self._runnable.executed)
            self.assertTrue(self._runnable.stopped)

            runner.join()

            self.assertTrue(runner.started)
            self.assertTrue(runner.joined)
            self.assertFalse(runner.is_alive())
            self.assertTrue(self._runnable.executed)
            self.assertTrue(self._runnable.stopped)

    @assert_no_warnings_or_errors_logged
    def test_clean_shutdown_by_context_management(self) -> None:
        runner = TestInlineRunner(self._runnable, None)
        cm = runner.__enter__()
        cm.start()
        cm.__exit__(None, None, None)

        self.assertTrue(runner.joined)
        self.assertFalse(runner.is_alive())
        self.assertTrue(self._runnable.executed)
        self.assertTrue(self._runnable.stopped)

    def test_context_management_when_run_raises_exception(self) -> None:
        # Errors from runners can't be thrown to the owning thread or process. The Runnable may choose to capture and pass them out in a "completed" message,
        # or it may simply let the exception be raised, which will be caught by the Runner. In this case, the runner will send a "completed" message back to the owner,
        # containing the exception.
        # Errors in the status buffer would normally be received and re-raised in the caller's scope by the caller polling "check_for_exceptions", but to ensure that
        # exceptions are not lost, this call is also made in the API, for instance when the runner goes out of context management.
        erroring_runnable = TestInlineRunnable("Test", raise_error=True)
        runner = TestInlineRunner(erroring_runnable, None)
        cm = runner.__enter__()
        cm.start()
        with self.assertRaisesRegex(RuntimeError, "Test Error"):
            cm.__exit__(None, None, None)

    @assert_no_warnings_or_errors_logged
    def test_join_timeout_during_shutdown_by_context_management(self) -> None:
        immortal_runnable = TestInlineRunnable("Test", simulate_stop_not_working=True)
        runner = TestInlineRunner(immortal_runnable, None)
        cm = runner.__enter__()
        cm.start()
        with self.assertRaisesRegex(RunnerStillAliveError, "Failed to stop the runner"):
            cm.__exit__(None, None, None)

        # Check that the __exit__() call did exit the context management, even though it raised an exception
        with self.assertRaisesRegex(MustBeContextManagedError, "Must be context managed"):
            runner.start()

    @assert_no_warnings_or_errors_logged
    def test_status_buffer_context_management(self) -> None:
        # StatusBuffer has its own test case. The 'test_status_buffer_' tests in this class check that the status buffer is being correctly driven by the Runner.
        # There are no equivalent tests for the command buffer because that has no higher-level wrapper equivalent to StatusBuffer, it is just a plain buffer.
        runner = TestInlineRunner(self._runnable, None)

        self.assertEqual(TST_BUFFER_SIZES, runner.get_status_buffer().values.maxlen)
        self.assertFalse(runner.get_status_buffer().entered)
        self.assertFalse(runner.get_status_buffer().exited)

        cm = runner.__enter__()

        self.assertTrue(runner.get_status_buffer().entered)
        self.assertFalse(runner.get_status_buffer().exited)

        cm.__exit__(None, None, None)

        self.assertTrue(runner.get_status_buffer().entered)
        self.assertTrue(runner.get_status_buffer().exited)

    @assert_no_warnings_or_errors_logged
    def test_status_buffer_messages_when_run_succeeds(self) -> None:
        with TestInlineRunner(self._runnable, None) as runner:
            runner.start()
            self.assertEqual("[ValueItem: StartedStatusMessage(), CompleteItem (no error)]", list_str(list(runner.get_status_buffer().values)))

            runner.get_status_buffer().values.clear()  # to prevent stop() from raising an exception
            runner.stop()

    def test_status_buffer_messages_when_run_raises_exception(self) -> None:
        erroring_runnable = TestInlineRunnable("Test", raise_error=True)
        with TestInlineRunner(erroring_runnable, None) as runner:
            runner.start()
            self.assertEqual("[ValueItem: StartedStatusMessage(), CompleteItem (with error 'TraceableException(RuntimeError('Test Error'))')]",
                             list_str(list(runner.get_status_buffer().values)))

            runner.get_status_buffer().values.clear()  # to prevent stop() from raising an exception
            runner.stop()

    # noinspection PyTypeChecker
    @no_type_check
    def test_illegal_params_thread_runner(self) -> None:
        with self.assertRaises(ValueError):
            ThreadRunner(None, "Name")
        with ThreadRunner(self._runnable, "Name") as runner:
            with self.assertRaises(ValueError):
                runner.set_name(None)
            runner.start()
            with self.assertRaises(ValueError):
                runner.join("string")
            with self.assertRaises(ValueError):
                runner.join(-1.0)
            runner.join(1.0)
            with self.assertRaises(ValueError):
                runner.start_blocking("string")
            with self.assertRaises(ValueError):
                runner.start_blocking(-1.0)
            with self.assertRaises(ValueError):
                runner.wait_until_running("string")
            with self.assertRaises(ValueError):
                runner.wait_until_running(-1.0)

    # noinspection PyTypeChecker
    @no_type_check
    def test_illegal_params_process_runner(self) -> None:
        # NOTE: If this test fails when all others are passing, it is probably because this is the ONLY test in the "fast" tests which actually starts
        # another process, even though that is not its primary function; and so this probably indicates an issue with pickling.
        with self.assertRaises(ValueError):
            ProcessRunner(None, "Name")
        with ProcessRunner(self._runnable, "Name") as runner:
            with self.assertRaises(ValueError):
                runner.set_name(None)
            runner.start()
            with self.assertRaises(ValueError):
                runner.join("string")
            with self.assertRaises(ValueError):
                runner.join(-1.0)
            runner.join(10.0)
            with self.assertRaises(ValueError):
                runner.start_blocking("string")
            with self.assertRaises(ValueError):
                runner.start_blocking(-1.0)
            with self.assertRaises(ValueError):
                runner.wait_until_running("string")
            with self.assertRaises(ValueError):
                runner.wait_until_running(-1.0)
