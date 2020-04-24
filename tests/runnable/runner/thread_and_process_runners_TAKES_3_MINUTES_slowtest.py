import logging
import time
from unittest import TestCase

from puma.runnable.runner.runner import DEFAULT_FINAL_JOIN_TIMEOUT, RunnerStillAliveError
from tests.parameterized import parameterized
from tests.runnable.runner.test_blocking_runnable import TestBlockingRunnable
from tests.runnable.runner.test_execution_mode import TestExecutionMode
from tests.runnable.test_support.parameterisation import RunnerTestParams, envs
from tests.test_logging_helpers import assert_no_warnings_or_errors_logged

logger = logging.getLogger(__name__)


class ThreadAndProcessRunner_Takes3Minutes_SlowTest(TestCase):
    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_name_supplied(self, param: RunnerTestParams) -> None:
        env = param._env
        ran_event = env.create_event()
        ended_event = env.create_event()
        runnable = TestBlockingRunnable("Test", stop_event=env.create_event(), ran_event=ran_event, ended_event=ended_event)
        with env.create_runner(runnable, "abuff_name") as runner:
            self.assertEqual("abuff_name", runner.get_name())

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_name_not_supplied(self, param: RunnerTestParams) -> None:
        env = param._env
        ran_event = env.create_event()
        ended_event = env.create_event()
        runnable = TestBlockingRunnable("Test", stop_event=env.create_event(), ran_event=ran_event, ended_event=ended_event)
        with env.create_runner(runnable, None) as runner:
            self.assertEqual(env.runner_class_name() + " of Test", runner.get_name())

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_when_run_waits_until_stopped(self, param: RunnerTestParams) -> None:
        env = param._env
        ran_event = env.create_event()
        ended_event = env.create_event()
        timeout = env.activity_timeout()
        runnable = TestBlockingRunnable("Test", stop_event=env.create_event(), ran_event=ran_event, ended_event=ended_event,
                                        execution_mode=TestExecutionMode.WaitUntilStopped)
        with env.create_runner(runnable, None) as runner:
            self.assertFalse(ran_event.wait(timeout))  # expecting this to time out, we haven't been started
            runner.start()
            self.assertTrue(ran_event.wait(timeout))

            self.assertFalse(ended_event.wait(timeout))  # expecting this to time out, we haven't been stopped
            runner.stop()
            self.assertTrue(ended_event.wait(timeout))

            runner.join(env.activity_timeout())
            self.assertFalse(runner.is_alive())

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_when_run_ends_quickly(self, param: RunnerTestParams) -> None:
        env = param._env
        ran_event = env.create_event()
        ended_event = env.create_event()
        timeout = env.activity_timeout()
        runnable = TestBlockingRunnable("Test", stop_event=env.create_event(), ran_event=ran_event, ended_event=ended_event,
                                        execution_mode=TestExecutionMode.EndQuicklyWithoutError)
        with env.create_runner(runnable, None) as runner:
            self.assertFalse(ran_event.wait(timeout))  # expecting this to time out, we haven't been started
            runner.start()
            self.assertTrue(ran_event.wait(timeout))
            self.assertTrue(ended_event.wait(timeout))

            runner.stop()  # no effect

            runner.join(env.activity_timeout())
            self.assertFalse(runner.is_alive())

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_when_run_ends_quickly_no_stop(self, param: RunnerTestParams) -> None:
        env = param._env
        ran_event = env.create_event()
        ended_event = env.create_event()
        timeout = env.activity_timeout()
        runnable = TestBlockingRunnable("Test", stop_event=env.create_event(), ran_event=ran_event, ended_event=ended_event,
                                        execution_mode=TestExecutionMode.EndQuicklyWithoutError)
        with env.create_runner(runnable, None) as runner:
            self.assertFalse(ran_event.wait(timeout))  # expecting this to time out, we haven't been started
            runner.start()
            self.assertTrue(ran_event.wait(timeout))
            self.assertTrue(ended_event.wait(timeout))

            runner.join(env.activity_timeout())

    @parameterized(envs)
    def test_when_run_ends_with_error(self, param: RunnerTestParams) -> None:
        env = param._env
        ran_event = env.create_event()
        ended_event = env.create_event()
        timeout = env.activity_timeout()
        runnable = TestBlockingRunnable("Test", stop_event=env.create_event(), ran_event=ran_event, ended_event=ended_event,
                                        execution_mode=TestExecutionMode.EndQuicklyWithError)
        with env.create_runner(runnable, None) as runner:
            self.assertFalse(ran_event.wait(timeout))  # expecting this to time out, we haven't been started
            runner.start()
            self.assertTrue(ran_event.wait(timeout))
            self.assertTrue(ended_event.wait(timeout))

            runner.stop()  # no effect
            runner.join(env.activity_timeout())
            self.assertFalse(runner.is_alive())

            with self.assertRaisesRegex(RuntimeError, "Test Error"):
                runner.check_for_exceptions()

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_wait_until_running_when_run_waits_until_stopped(self, param: RunnerTestParams) -> None:
        env = param._env
        ran_event = env.create_event()
        ended_event = env.create_event()
        timeout = env.activity_timeout()
        runnable = TestBlockingRunnable("Test", stop_event=env.create_event(), ran_event=ran_event, ended_event=ended_event,
                                        execution_mode=TestExecutionMode.WaitUntilStopped)
        with env.create_runner(runnable, None) as runner:
            self.assertFalse(ran_event.wait(timeout))  # expecting this to time out, we haven't been started
            runner.start()
            runner.wait_until_running(timeout)
            self.assertTrue(ran_event.wait(timeout))

            runner.stop()
            runner.join(env.activity_timeout())
            self.assertFalse(runner.is_alive())

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_start_blocking_when_run_waits_until_stopped(self, param: RunnerTestParams) -> None:
        env = param._env
        ran_event = env.create_event()
        ended_event = env.create_event()
        timeout = env.activity_timeout()
        runnable = TestBlockingRunnable("Test", stop_event=env.create_event(), ran_event=ran_event, ended_event=ended_event,
                                        execution_mode=TestExecutionMode.WaitUntilStopped)
        with env.create_runner(runnable, None) as runner:
            self.assertFalse(ran_event.wait(timeout))  # expecting this to time out, we haven't been started
            runner.start_blocking(env.activity_timeout())
            self.assertTrue(ran_event.wait(timeout))

            runner.stop()
            runner.join(env.activity_timeout())
            self.assertFalse(runner.is_alive())

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_wait_until_running_when_run_ends_quickly(self, param: RunnerTestParams) -> None:
        env = param._env
        ran_event = env.create_event()
        ended_event = env.create_event()
        timeout = env.activity_timeout()
        runnable = TestBlockingRunnable("Test", stop_event=env.create_event(), ran_event=ran_event, ended_event=ended_event,
                                        execution_mode=TestExecutionMode.EndQuicklyWithoutError)
        with env.create_runner(runnable, None) as runner:
            self.assertFalse(ran_event.wait(timeout))  # expecting this to time out, we haven't been started
            runner.start()
            runner.wait_until_running(timeout)
            self.assertTrue(ran_event.wait(timeout))
            self.assertTrue(ended_event.wait(timeout))

            runner.join(env.activity_timeout())
            self.assertFalse(runner.is_alive())

    @parameterized(envs)
    def test_scope_ended_while_running_runnable_stops(self, param: RunnerTestParams) -> None:
        # If a join() times out and we didn't notice, then the runnable will typically go out of scope while running; we need to treat this as an error
        # It will take a long time (by default, 30 seconds) for the runner to conclude that the runnable is not going to end.
        # The runner tells the runnable to stop, so it should end quickly and without error.
        env = param._env
        runnable = TestBlockingRunnable("Test", stop_event=env.create_event(), execution_mode=TestExecutionMode.WaitUntilStopped)
        with env.create_runner(runnable, None) as runner:
            runner.start()
            t1 = time.monotonic()
        t2 = time.monotonic()
        self.assertLess(t2 - t1, env.activity_timeout())

    @parameterized(envs)
    def test_scope_ended_while_running_runnable_does_not_stop_takes_30_seconds(self, param: RunnerTestParams) -> None:
        # If a join() times out and we didn't notice, then the runnable will typically go out of scope while running; we need to treat this as an error
        # In this test, we tell the runnable to never end (ignore the stop command).
        # It will take a long time (by default, 30 seconds) for the runner to conclude that the runnable is not going to end.
        env = param._env
        runnable = TestBlockingRunnable("Test", stop_event=env.create_event(), execution_mode=TestExecutionMode.IgnoreStopCommand)
        with self.assertRaisesRegex(RuntimeError, "Failed to stop the runner"):
            with env.create_runner(runnable, None) as runner:
                runner.start()
                t1 = time.monotonic()
        t2 = time.monotonic()
        self.assertGreaterEqual(t2 - t1, DEFAULT_FINAL_JOIN_TIMEOUT)
        self.assertLess(t2 - t1, DEFAULT_FINAL_JOIN_TIMEOUT + 3.0)

    @parameterized(envs)
    def test_scope_ended_while_running_due_to_exception_runnable_does_not_stop_takes_30_seconds(self, param: RunnerTestParams) -> None:
        # If a join() times out and we responded by raising en exception, then the runnable will go out of scope while running;
        # since an error is already raised, we shouldn't make things more complicated by raising another.
        # In this test, we tell the runnable to never end (ignore the stop command).
        # It will take a long time (by default, 30 seconds) for the runner to conclude that the runnable is not going to end.
        env = param._env
        runnable = TestBlockingRunnable("Test", stop_event=env.create_event(), execution_mode=TestExecutionMode.IgnoreStopCommand)
        with self.assertRaisesRegex(RunnerStillAliveError, "Failed to stop the runner: (ThreadRunner|ProcessRunner) of Test"):
            with env.create_runner(runnable, None) as runner:
                runner.start()
                t1 = time.monotonic()
                runner.join(0.0)

        t2 = time.monotonic()
        self.assertGreaterEqual(t2 - t1, DEFAULT_FINAL_JOIN_TIMEOUT)
        self.assertLess(t2 - t1, DEFAULT_FINAL_JOIN_TIMEOUT + 3.0)
