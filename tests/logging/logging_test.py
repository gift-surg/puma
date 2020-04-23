import logging
import os
import tempfile
from pathlib import Path
from unittest import TestCase

import yaml

from puma.logging import LogLevel, Logging
from tests.logging.capture_logs import CaptureLogs, CapturedRecords


# Basic tests of LoggingUtils.
# More thorough tests in the case of multiprocess logging, including the use of rotating log files, is in thread_and_process_runners_logging_slowtest.py.


class LoggingTest(TestCase):

    def __init__(self, methodName: str = 'runTest') -> None:
        super().__init__(methodName)
        Logging.reset_logging()  # in case some previous test case has messed up the logging
        my_folder = os.path.dirname(os.path.realpath(__file__))
        self._test_config = str(Path(my_folder).joinpath('logging_test.yaml'))

    def setUp(self) -> None:
        Logging.init_logging(self._test_config)
        self._logger = logging.getLogger(__name__)

    def tearDown(self) -> None:
        Logging.reset_logging()

    def test_basics(self) -> None:
        unconfigured_logger = logging.getLogger('a.b.c')
        with CaptureLogs() as log_context:
            self._logger.debug("Debug message 1")
            logging.debug("Root debug message")
            warn_level_logger = logging.getLogger('level.logger')
            warn_level_logger.debug('Debug message 2')
            warn_level_logger.warning('Warning message 1')
            warn_level_logger.error('Error message 1')
            unconfigured_logger.debug('Unconfigured debug message')

            records = log_context.pop_captured_records()
            self._assert_captured_once(records, "Debug message 1")
            self._assert_captured_once(records, "Root debug message")
            self._assert_not_captured(records, "Debug message 2")
            self._assert_captured_once(records, "Warning message 1")
            self._assert_captured_once(records, "Error message 1")
            self._assert_captured_once(records, "Unconfigured debug message")

    def test_init_overrides_level_of_existing_child_logger(self) -> None:
        child_logger = logging.getLogger('tests.child')
        child_logger.setLevel(logging.WARNING)
        Logging.init_logging(self._test_config)
        with CaptureLogs() as log_context:
            child_logger.debug("Debug message")
            records = log_context.pop_captured_records()
            self._assert_captured_once(records, "Debug message")

    def test_override_global_level_up(self) -> None:
        unconfigured_logger = logging.getLogger('a.b.c')
        Logging.override_global_level(LogLevel.error)
        with CaptureLogs() as log_context:
            self._logger.debug("Debug message 1")
            logging.debug("Root debug message")
            level_logger = logging.getLogger('level.logger')
            level_logger.debug('Debug message 2')
            level_logger.warning('Warning message 1')
            level_logger.error('Error message 1')
            unconfigured_logger.debug('Unconfigured debug message')

            records = log_context.pop_captured_records()
            self._assert_not_captured(records, "Debug message 1")
            self._assert_not_captured(records, "Root debug message")
            self._assert_not_captured(records, "Debug message 2")
            self._assert_not_captured(records, "Warning message 1")
            self._assert_captured_once(records, "Error message 1")
            self._assert_not_captured(records, "Unconfigured debug message")

    def test_override_global_level_down(self) -> None:
        unconfigured_logger = logging.getLogger('a.b.c')
        Logging.override_global_level(LogLevel.debug)
        with CaptureLogs() as log_context:
            self._logger.debug("Debug message 1")
            logging.debug("Root debug message")
            level_logger = logging.getLogger('level.logger')
            level_logger.debug('Debug message 2')
            level_logger.warning('Warning message 1')
            level_logger.error('Error message 1')
            unconfigured_logger.debug('Unconfigured debug message')

            records = log_context.pop_captured_records()
            self._assert_captured_once(records, "Debug message 1")
            self._assert_captured_once(records, "Root debug message")
            self._assert_captured_once(records, "Debug message 2")
            self._assert_captured_once(records, "Warning message 1")
            self._assert_captured_once(records, "Error message 1")
            self._assert_captured_once(records, "Unconfigured debug message")

    def test_override_sections(self) -> None:
        unconfigured_logger = logging.getLogger('a.b.c')
        Logging.override_sections({'tests': LogLevel.warn, 'level.logger': LogLevel.error})
        with CaptureLogs() as log_context:
            self._logger.debug("Debug message 1")
            logging.debug("Root debug message")
            error_level_logger = logging.getLogger('level.logger')
            error_level_logger.debug('Debug message 2')
            error_level_logger.warning('Warning message 1')
            error_level_logger.error('Error message 1')
            unconfigured_logger.debug('Unconfigured debug message')

            records = log_context.pop_captured_records()
            self._assert_not_captured(records, "Debug message 1")
            self._assert_captured_once(records, "Root debug message")
            self._assert_not_captured(records, "Debug message 2")
            self._assert_not_captured(records, "Warning message 1")
            self._assert_captured_once(records, "Error message 1")
            self._assert_captured_once(records, "Unconfigured debug message")

    def test_override_root(self) -> None:
        unconfigured_logger = logging.getLogger('a.b.c')
        Logging.override_root(LogLevel.error)
        with CaptureLogs() as log_context:
            self._logger.debug("Debug message 1")
            logging.debug("Root debug message")
            warn_level_logger = logging.getLogger('level.logger')
            warn_level_logger.debug('Debug message 2')
            warn_level_logger.warning('Warning message 1')
            warn_level_logger.error('Error message 1')
            unconfigured_logger.debug('Unconfigured debug message')

            records = log_context.pop_captured_records()
            self._assert_captured_once(records, "Debug message 1")
            self._assert_not_captured(records, "Root debug message")
            self._assert_not_captured(records, "Debug message 2")
            self._assert_captured_once(records, "Warning message 1")
            self._assert_captured_once(records, "Error message 1")
            self._assert_not_captured(records, "Unconfigured debug message")

    def test_reset_logging(self) -> None:
        with CaptureLogs() as log_context:
            logger1 = logging.getLogger('testy')
            logger1.warning("testy1")  # logged, because 'testy' unknown, and root logger is DEBUG level thanks to loading from logging_test.yaml file
            records = log_context.pop_captured_records()
            self._assert_captured_once(records, "testy1")

        Logging.override_sections({'testy': LogLevel.error})

        with CaptureLogs() as log_context:
            logger1 = logging.getLogger('testy')
            logger1.warning("testy2")  # not logged, because 'testy' is at ERROR level
            records = log_context.pop_captured_records()
            self._assert_not_captured(records, "testy2")

        Logging.reset_logging()  # 'testy' logger forgotten, and logging set to WARNING

        with CaptureLogs() as log_context:
            logger1 = logging.getLogger('testy')
            logger1.warning("testy3")  # logged, because 'testy' unknown, and root logger is WARNING level
            records = log_context.pop_captured_records()
            self._assert_captured_once(records, "testy3")

    def test_restore_logging(self) -> None:
        with CaptureLogs() as log_context:
            configuration = Logging.get_current_logging_config()
            logger1 = logging.getLogger('testy')
            logger1.debug("Before reset")
            Logging.reset_logging()
            logger1.debug("After reset, before restore")  # will be lost
            Logging.restore_current_logging_config(configuration)
            logger1.debug("After restore")
            records = log_context.pop_captured_records()
            self.assertEqual(['Before reset', 'After restore'], records.get_lines(timestamp=False, level=False, line_separators=False))

    def test_logging_to_file(self) -> None:
        logger1 = logging.getLogger(__name__)

        my_folder = os.path.dirname(os.path.realpath(__file__))
        test_config_path = Path(my_folder).joinpath('logging_to_files_test.yaml')
        with test_config_path.open('rt') as f:
            test_config = yaml.safe_load(f.read())

        handle, temp_filename = tempfile.mkstemp(text=True)
        os.close(handle)
        temp_path = Path(temp_filename)
        try:
            test_config['handlers']['to_file']['filename'] = temp_filename
            Logging.init_logging_from_dict(test_config)

            logger1.debug("Test output")

            Logging.reset_logging()  # closes the log file

            if not temp_path.is_file():
                self.fail("Failed to create the log file")

            with temp_path.open('rt') as log_file:
                log_content = log_file.readlines()
            self.assertEqual(["Test output\n"], log_content)
        finally:
            temp_path.unlink()

    def _assert_captured_once(self, records: CapturedRecords, content: str) -> None:
        self.assertEqual([content], records.containing_message(content).get_lines(timestamp=False, level=False))

    def _assert_not_captured(self, records: CapturedRecords, content: str) -> None:
        self.assertFalse(records.containing_message(content))
