import logging
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from unittest import TestCase

import yaml

from puma.attribute import copied
from puma.logging import LogLevel, Logging, ManagedProcessLogQueue
from puma.runnable import Runnable
from puma.runnable.runner import ProcessRunner
from tests.logging.capture_logs import CaptureLogs, CapturedRecords
from tests.mixin import NotATestCase
from tests.parameterized import parameterized
from tests.runnable.runner.test_logging_runnable import TestLoggingRunnable
from tests.runnable.test_support.parameterisation import ProcessRunnerTestEnvironment, RunnerTestParams, envs


class TestProcessRunner(ProcessRunner, NotATestCase):
    """Derived version of ProcessRunner that allows us to mess about with the process logging mechanism."""

    def __init__(self, runnable: Runnable, name: Optional[str] = None, *, log_queue_size: int = 0):
        self._log_queue_size = log_queue_size
        super().__init__(runnable, name)

    def __enter__(self) -> 'TestProcessRunner':
        super().__enter__()
        return self

    def _log_queue_factory(self) -> ManagedProcessLogQueue:
        return ManagedProcessLogQueue(maxsize=self._log_queue_size, name='logging queue')

    @staticmethod
    def pause_log_listener_process() -> None:
        assert ProcessRunner._process_logging_mechanism
        ProcessRunner._process_logging_mechanism.pause_listener_process()

    @staticmethod
    def resume_log_listener_process() -> None:
        assert ProcessRunner._process_logging_mechanism
        ProcessRunner._process_logging_mechanism.resume_listener_process()


class ThreadAndProcessRunnerLoggingSlowTest(TestCase):
    def setUp(self) -> None:
        Logging.reset_logging()  # in case some previous test case has messed up the logging

        my_folder = os.path.dirname(os.path.realpath(__file__))
        log_config = Path(my_folder).joinpath('process_logging_test.yaml')
        Logging.init_logging(str(log_config))

    def tearDown(self) -> None:
        Logging.reset_logging()

    @parameterized(envs)
    def test_child_thread_or_process(self, param: RunnerTestParams) -> None:
        env = param._env
        runnable = TestLoggingRunnable("Test")
        with CaptureLogs(LogLevel.debug) as logging_context:
            with env.create_runner(runnable, None) as runner:
                runner.start_blocking()
                runner.join(env.activity_timeout())
                self.assertFalse(runner.is_alive())
                runner.check_for_exceptions()
                records = logging_context.pop_captured_records()
                self._assert_captured_once(records.with_levels_in({LogLevel.debug}), "Debug message")
                self._assert_captured_once(records.with_levels_in({LogLevel.warn}), "Warning message")
                self._assert_captured_once(records.with_levels_in({LogLevel.error}), "Error message")

                self.assertFalse(records.from_package('puma'))  # shouldn't be getting logging from non-test code, logging_dev.yaml configuration sets it to warning level

    def test_grandchild_process(self) -> None:
        self._run_runnable_that_creates_child_process_recursively(depth=1)

    def test_greatgrandchild_process(self) -> None:
        self._run_runnable_that_creates_child_process_recursively(depth=2)

    def _run_runnable_that_creates_child_process_recursively(self, depth: int) -> None:
        runnable = TestLoggingRunnable("Test", launch_child_runners_depth=depth)
        with CaptureLogs(LogLevel.debug) as logging_context:
            with ProcessRunner(runnable, None) as runner:
                runner.start_blocking()
                runner.join(ProcessRunnerTestEnvironment().activity_timeout() * (depth + 1))
                self.assertFalse(runner.is_alive())
                runner.check_for_exceptions()
                records = logging_context.pop_captured_records()
                for d in range(1, depth):
                    self._assert_captured_once(records.with_levels_in({LogLevel.debug}), f"Debug message at depth {d}")
                self.assertFalse(records.from_package('puma'))  # shouldn't be getting logging from non-test code, logging_dev.yaml configuration sets it to warning level

    def test_logging_captured_within_child_process(self) -> None:
        runnable = TestLoggingRunnable("Test", capture_and_relog=True)
        with CaptureLogs(LogLevel.debug) as logging_context:
            with ProcessRunner(runnable, None) as runner:
                runner.start_blocking()
                runner.join(20.0)
                self.assertFalse(runner.is_alive())
                runner.check_for_exceptions()
                records = logging_context.pop_captured_records()
                self._assert_captured_once(records.with_levels_in({LogLevel.debug}), "!!Debug message")
                self.assertFalse(records.from_package('puma'))  # shouldn't be getting logging from non-test code, logging_dev.yaml configuration sets it to warning level

    def test_records_are_filtered_before_queuing(self) -> None:
        # Make sure that we don't push irrelevant log records on to the queue, only to have them discarded when received.
        # To test this, we give the queue a small size, and stop the listening process, before doing lots of irrelevant logging and some relevant logging.
        # If the records are not being filtered then the queue will fill up.
        # Otherwise, when we restart the logging process, we should get the relevant logs.
        queue_size = 5
        runnable = TestLoggingToPumaRunnable("Test", debug_log_count=queue_size * 2)
        with CaptureLogs(LogLevel.debug) as logging_context:
            with TestProcessRunner(runnable, None, log_queue_size=queue_size) as runner:
                runner.pause_log_listener_process()

                runner.start_blocking()
                runner.join(20.0)
                self.assertFalse(runner.is_alive())
                runner.check_for_exceptions()

                runner.resume_log_listener_process()
                records = logging_context.pop_captured_records()
                self.assertFalse(records.containing_message("Filtered out"))
                self._assert_captured_once(records, "Relevant")

    def test_logging_to_file_from_processes_delay_is_false(self) -> None:
        # With the "delay" option false, the log file is created as soon as logging is configured
        # Launches a child process only
        self._test_logging_to_file_from_processes_impl(rotating_files=False, file_open_delay_option=False, child_processes_depth=0)

    def test_logging_to_file_from_processes_delay_is_true(self) -> None:
        # With the "delay" option true, the log file is created when the first output is written to it
        # Launches a child process only
        self._test_logging_to_file_from_processes_impl(rotating_files=False, file_open_delay_option=True, child_processes_depth=0)

    def test_logging_to_file_from_processes_delay_is_false_deep(self) -> None:
        # With the "delay" option false, the log file is created as soon as logging is configured
        # Launches child, grandchild and great-grandchild processes
        self._test_logging_to_file_from_processes_impl(rotating_files=False, file_open_delay_option=False, child_processes_depth=2)

    def test_logging_to_file_from_processes_delay_is_true_deep(self) -> None:
        # With the "delay" option true, the log file is created when the first output is written to it
        # Launches child, grandchild and great-grandchild processes
        self._test_logging_to_file_from_processes_impl(rotating_files=False, file_open_delay_option=True, child_processes_depth=2)

    def test_logging_to_rotating_files_from_processes_delay_is_false(self) -> None:
        # Logging to multiple files, rotating every few seconds
        # With the "delay" option false, the log file is created as soon as logging is configured
        # Launches a child process only
        self._test_logging_to_file_from_processes_impl(rotating_files=True, file_open_delay_option=False, child_processes_depth=0)

    def test_logging_to_rotating_files_from_processes_delay_is_false_with_delay(self) -> None:
        # Regression test: same as the test above, but with a delay after the child process finishes. It was accidentally found
        # that this broke the rotating-files logging.
        self._test_logging_to_file_from_processes_impl(rotating_files=True, file_open_delay_option=False, child_processes_depth=0, post_child_delay=True)

    def test_logging_to_rotating_files_from_processes_delay_is_true(self) -> None:
        # Logging to multiple files, rotating every few seconds
        # With the "delay" option true, the log file is created when the first output is written to it
        # Launches a child process only
        self._test_logging_to_file_from_processes_impl(rotating_files=True, file_open_delay_option=True, child_processes_depth=0)

    def test_logging_to_rotating_files_from_processes_delay_is_false_deep(self) -> None:
        # Logging to multiple files, rotating every few seconds
        # With the "delay" option false, the log file is created as soon as logging is configured
        # Launches child, grandchild and great-grandchild processes
        self._test_logging_to_file_from_processes_impl(rotating_files=True, file_open_delay_option=False, child_processes_depth=2)

    def test_logging_to_rotating_files_from_processes_delay_is_true_deep(self) -> None:
        # Logging to multiple files, rotating every few seconds
        # With the "delay" option true, the log file is created when the first output is written to it
        # Launches child, grandchild and great-grandchild processes
        self._test_logging_to_file_from_processes_impl(rotating_files=True, file_open_delay_option=True, child_processes_depth=2)

    def _test_logging_to_file_from_processes_impl(self, *, rotating_files: bool, file_open_delay_option: bool, child_processes_depth: int, post_child_delay: bool = False) -> None:
        logger1 = logging.getLogger(__name__)

        if rotating_files:
            config_filename = 'process_logging_to_rotating_files_test.yaml'
        else:
            config_filename = 'process_logging_to_files_test.yaml'

        temp_path, test_config = self._configure_logging_to_file(config_filename, file_open_delay_option)

        try:
            Logging.init_logging_from_dict(test_config)

            logger1.debug("Main process debug 1")

            runnable = TestLoggingRunnable("Test", launch_child_runners_depth=child_processes_depth, delay=2.0)
            with TestProcessRunner(runnable) as runner:
                runner.start_blocking()

                # The child process is running, currently sleeping for a couple of seconds before logging. Let's do some logging from the parent while in that state.
                logger1.debug("Main process debug 2")

                runner.join(10.0)
                self.assertFalse(runner.is_alive())
                runner.check_for_exceptions()

            if post_child_delay:
                # Regression testing: it was accidentally discovered that a delay here broke the system in the case of logging to rotating files,
                # losing the log output from the child process and also "Main process debug 2", this apparently being written over by the ""Main process debug 3" below.
                time.sleep(12.0)

            logger1.debug("Main process debug 3")

            Logging.reset_logging()  # closes the log file(s)

            if rotating_files:
                self._verify_rotating_log_files_content(child_processes_depth, temp_path)
            else:
                self._verify_log_file_content(child_processes_depth, temp_path)
        finally:
            Logging.reset_logging()  # closes the log file(s)
            self._delete_log_files(temp_path, rotating_files)

    @staticmethod
    def _delete_log_files(temp_path: Path, rotating_files: bool) -> None:
        if rotating_files:
            log_files = ThreadAndProcessRunnerLoggingSlowTest._find_log_files_in_order(temp_path)
            for log_file in log_files:
                log_file.unlink()
        else:
            temp_path.unlink()

    def _verify_log_file_content(self, child_processes_depth: int, log_file_path: Path) -> None:
        if not log_file_path.is_file():
            self.fail("Failed to create the log file")

        with log_file_path.open('rt') as log_file:
            log_content = log_file.readlines()

        expected_log_lines = self._get_expected_log_file_content(child_processes_depth)
        self.assertEqual(expected_log_lines, log_content)

    @staticmethod
    def _get_expected_log_file_content(child_processes_depth: int) -> List[str]:
        expected_log_lines = ['Main process debug 1\n',
                              'Main process debug 2\n',
                              'Debug message\n',
                              'Warning message\n',
                              'Error message\n']
        for child_depth in range(1, child_processes_depth + 1):
            expected_log_lines.extend(['Debug message\n',
                                       'Warning message\n',
                                       'Error message\n',
                                       f'Debug message at depth {child_depth}\n'])
        expected_log_lines.append('Main process debug 3\n')
        return expected_log_lines

    def _verify_rotating_log_files_content(self, child_processes_depth: int, log_file_path_root: Path) -> None:
        log_files = self._find_log_files_in_order(log_file_path_root)

        all_lines = []
        for log_file in log_files:
            with log_file.open('rt') as log:
                log_content = log.readlines()
                all_lines.extend(log_content)

        expected_log_lines = self._get_expected_log_file_content(child_processes_depth)
        self.assertEqual(expected_log_lines, all_lines)

    @staticmethod
    def _find_log_files_in_order(log_file_path_root: Path) -> List[Path]:
        log_files: List[Path] = []

        # First find "backup" files
        directory = log_file_path_root.parent
        backup_file_names_prefix = log_file_path_root.name + "."
        for root, dirs, files in os.walk(directory):
            for name in files:
                if name.startswith(backup_file_names_prefix):
                    log_files.append(Path(os.path.join(root, name)))

        # sort the backup files, which are timestamped in a way that allows them to be sorted in order
        log_files.sort()

        # If there is a "current" file then add it at the end
        if log_file_path_root.is_file():
            log_files.append(log_file_path_root)

        return log_files

    @staticmethod
    def _configure_logging_to_file(config_filename: str, file_open_delay_option: bool) -> Tuple[Path, Dict[str, Any]]:
        handle, temp_filename = tempfile.mkstemp(text=True)
        os.close(handle)
        temp_path = Path(temp_filename)

        my_folder = os.path.dirname(os.path.realpath(__file__))
        test_config_path = Path(my_folder).joinpath(config_filename)
        with test_config_path.open('rt') as f:
            test_config = yaml.safe_load(f.read())

        test_config['handlers']['to_file']['filename'] = temp_filename
        test_config['handlers']['to_file']['delay'] = file_open_delay_option

        return temp_path, test_config

    def _assert_captured_once(self, records: CapturedRecords, content: str) -> None:
        self.assertEqual([content], records.containing_message(content).get_lines(timestamp=False, level=False))

    def _assert_not_captured(self, records: CapturedRecords, content: str) -> None:
        self.assertFalse(records.containing_message(content))


class TestLoggingToPumaRunnable(Runnable, NotATestCase):
    """Logs to the puma namespace, which should be filtered at WARN level."""
    _debug_log_count: int = copied("_debug_log_count")

    def __init__(self, name: str, *, debug_log_count: int) -> None:
        super().__init__(name, [])
        self._debug_log_count = debug_log_count

    def _execute(self) -> None:
        puma_logger = logging.getLogger('puma')  # process_logging_test.yaml configures this for WARN level
        for i in range(self._debug_log_count):
            puma_logger.debug("Filtered out")

        tests_logger = logging.getLogger('tests')  # process_logging_test.yaml configures this for DEBUG level
        tests_logger.debug("Relevant")
