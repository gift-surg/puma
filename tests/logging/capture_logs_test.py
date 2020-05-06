import logging
import os
import tempfile
from pathlib import Path
from threading import Thread
from typing import Callable
from unittest import TestCase

from puma.helpers.testing.logging.capture_logs import CaptureLogs
from puma.logging import LogLevel, Logging
from tests.logging.capture_logs_test_secondary import log_in_secondary_module


class CaptureLogsTest(TestCase):

    def setUp(self) -> None:
        Logging.reset_logging()
        self._logger = logging.getLogger(__name__)
        self._logger.setLevel(logging.DEBUG)

    def tearDown(self) -> None:
        Logging.reset_logging()

    def test_get_lines(self) -> None:
        with CaptureLogs(LogLevel.debug) as log_context:
            self._logger.debug("Instance")
            records = log_context.pop_captured_records()
            self.assertEqual(['Instance'], records.get_lines(timestamp=False, level=False, line_separators=False))
            self.assertEqual(['DEBUG - Instance'], records.get_lines(timestamp=False, level=True, line_separators=False))
            self.assertEqual(['DEBUG - Instance\n'], records.get_lines(timestamp=False, level=True, line_separators=True))
            timestamped = records.get_lines(timestamp=True, level=False, line_separators=False)
            self.assertEqual(1, len(timestamped))
            self.assertRegex(timestamped[0], r'[0-9,\.]* - Instance')
            self.assertEqual(['**Instance'], records.get_lines(prefix='**', timestamp=False, level=False, line_separators=False))

    def test_containing_message(self) -> None:
        with CaptureLogs(LogLevel.debug) as log_context:
            self._logger.debug("Instance 1")
            self._logger.warning("Instance 2")
            self._logger.warning("An Inst")
            self._logger.warning("instance")
            self._logger.error("An Instance 3")
            records = log_context.pop_captured_records()
            self.assertEqual(['Instance 1', 'Instance 2', 'An Instance 3'],
                             records.containing_message("Instance").get_lines(timestamp=False, level=False, line_separators=False))

    def test_with_levels_in(self) -> None:
        with CaptureLogs(LogLevel.debug) as log_context:
            self._logger.debug("debug 1")
            self._logger.warning("warning 1")
            self._logger.error("error 1")
            self._logger.debug("debug 2")
            self._logger.warning("warning 2")
            self._logger.error("error 2")
            records = log_context.pop_captured_records()
            self.assertEqual(['debug 1', 'error 1', 'debug 2', 'error 2'],
                             records.with_levels_in({LogLevel.debug, LogLevel.error}).get_lines(timestamp=False, level=False, line_separators=False))

    def test_from_module(self) -> None:
        with CaptureLogs(LogLevel.debug) as log_context:
            self._logger.debug("debug 1")
            log_in_secondary_module("debug 2")
            records = log_context.pop_captured_records()
            self.assertEqual(['debug 2'],
                             records.from_module('capture_logs_test_secondary').get_lines(timestamp=False, level=False, line_separators=False))

    def test_from_package(self) -> None:
        logger_abc = logging.getLogger('a.b.c.file')
        logger_abc.setLevel(logging.DEBUG)
        with CaptureLogs(LogLevel.debug) as log_context:
            logger_abd = logging.getLogger('a.b.d.file')
            logger_abd.setLevel(logging.DEBUG)
            logger_xyz = logging.getLogger('x.y.z.file')
            logger_xyz.setLevel(logging.DEBUG)

            logger_abc.debug("debug 1")
            logger_abd.debug("debug 2")
            logger_xyz.debug("debug 3")
            records = log_context.pop_captured_records()
            self.assertEqual(['debug 1'],
                             records.from_package('a.b.c').get_lines(timestamp=False, level=False, line_separators=False))
            self.assertEqual(['debug 1', 'debug 2'],
                             records.from_package('a.b').get_lines(timestamp=False, level=False, line_separators=False))

    def test_log_content_default_is_debug(self) -> None:
        with CaptureLogs() as log_context:
            self._log_stuff()
            records = log_context.pop_captured_records()
            self.assertEqual(["This is debug"],
                             records.containing_message("This is debug").with_levels_in({LogLevel.debug}).get_lines(timestamp=False, level=False, line_separators=False))
            self.assertEqual(["This is warning"],
                             records.containing_message("This is warning").with_levels_in({LogLevel.warn}).get_lines(timestamp=False, level=False, line_separators=False))
            self.assertEqual(["This is error"],
                             records.containing_message("This is error").with_levels_in({LogLevel.error}).get_lines(timestamp=False, level=False, line_separators=False))

    def test_log_content_warn(self) -> None:
        with CaptureLogs(LogLevel.warn) as log_context:
            self._log_stuff()
            records = log_context.pop_captured_records()
            self.assertEqual([],
                             records.containing_message("This is debug").with_levels_in({LogLevel.debug}).get_lines(timestamp=False, level=False, line_separators=False))
            self.assertEqual(["This is warning"],
                             records.containing_message("This is warning").with_levels_in({LogLevel.warn}).get_lines(timestamp=False, level=False, line_separators=False))
            self.assertEqual(["This is error"],
                             records.containing_message("This is error").with_levels_in({LogLevel.error}).get_lines(timestamp=False, level=False, line_separators=False))

    def test_log_content_error(self) -> None:
        with CaptureLogs(LogLevel.error) as log_context:
            self._log_stuff()
            records = log_context.pop_captured_records()
            self.assertEqual([],
                             records.containing_message("This is debug").with_levels_in({LogLevel.debug}).get_lines(timestamp=False, level=False, line_separators=False))
            self.assertEqual([],
                             records.containing_message("This is warning").with_levels_in({LogLevel.warn}).get_lines(timestamp=False, level=False, line_separators=False))
            self.assertEqual(["This is error"],
                             records.containing_message("This is error").with_levels_in({LogLevel.error}).get_lines(timestamp=False, level=False, line_separators=False))

    def test_restricted_by_logger_level(self) -> None:
        with CaptureLogs(LogLevel.debug) as log_context:
            self._logger.setLevel(logging.DEBUG)
            self._logger.debug("debug 1")
            self._logger.warning("warning 1")

            self._logger.setLevel(logging.WARN)
            self._logger.debug("debug 2")
            self._logger.warning("warning 2")
            records = log_context.pop_captured_records()
            self.assertEqual(["debug 1", "warning 1", "warning 2"],
                             records.get_lines(timestamp=False, level=False, line_separators=False))

    def test_log_content_empty(self) -> None:
        with CaptureLogs() as log_context:
            records = log_context.pop_captured_records()
            self.assertFalse(records)  # calls CapturedRecords.__bool__
            self.assertEqual(0, len(records))

    def test_log_content_not_empty(self) -> None:
        with CaptureLogs() as log_context:
            self._log_stuff()
            records = log_context.pop_captured_records()
            self.assertTrue(records)  # calls CapturedRecords.__bool__
            self.assertEqual(3, len(records))

    def test_log_content_sequential(self) -> None:
        self._logger.debug("Message 1")
        with CaptureLogs() as log_context:
            self._logger.debug("Message 2")
            records = log_context.pop_captured_records()
            self.assertFalse(records.containing_message("Message 1").with_levels_in({LogLevel.debug}))
            self.assertEqual(["Message 2"],
                             records.containing_message("Message 2").with_levels_in({LogLevel.debug}).get_lines(timestamp=False, level=False, line_separators=False))
        self._logger.debug("Message 3")
        with CaptureLogs() as log_context:
            self._logger.debug("Message 4")
            records = log_context.pop_captured_records()
            self.assertFalse(records.containing_message("Message 1").with_levels_in({LogLevel.debug}))
            self.assertFalse(records.containing_message("Message 2").with_levels_in({LogLevel.debug}))
            self.assertFalse(records.containing_message("Message 3").with_levels_in({LogLevel.debug}))
            self.assertEqual(["Message 4"],
                             records.containing_message("Message 4").with_levels_in({LogLevel.debug}).get_lines(timestamp=False, level=False, line_separators=False))

    def test_log_content_sequential_with_reinitialise(self) -> None:
        # During testing, LoggingUtils.init_logging() may be called by each test, so the capture mechanism needs to cope with this
        Logging.init_logging()
        self._logger.debug("Message 1")
        with CaptureLogs() as log_context:
            self._logger.debug("Message 2")
            records = log_context.pop_captured_records()
            self.assertFalse(records.containing_message("Message 1").with_levels_in({LogLevel.debug}))
            self.assertEqual(["Message 2"],
                             records.containing_message("Message 2").with_levels_in({LogLevel.debug}).get_lines(timestamp=False, level=False, line_separators=False))

        Logging.init_logging()
        self._logger.debug("Message 3")
        with CaptureLogs() as log_context:
            self._logger.debug("Message 4")
            records = log_context.pop_captured_records()
            self.assertFalse(records.containing_message("Message 1").with_levels_in({LogLevel.debug}))
            self.assertFalse(records.containing_message("Message 2").with_levels_in({LogLevel.debug}))
            self.assertFalse(records.containing_message("Message 3").with_levels_in({LogLevel.debug}))
            self.assertEqual(["Message 4"],
                             records.containing_message("Message 4").with_levels_in({LogLevel.debug}).get_lines(timestamp=False, level=False, line_separators=False))

    def test_log_content_nested_without_nested_capture_context(self) -> None:
        self._logger.debug("Message 1")
        with CaptureLogs() as log_context_1:
            self._logger.debug("Message 2")
            with CaptureLogs() as log_context_2:
                self._logger.debug("Message 3")
                records = log_context_2.pop_captured_records()
                self.assertFalse(records.containing_message("Message 1").with_levels_in({LogLevel.debug}))
                self.assertFalse(records.containing_message("Message 2").with_levels_in({LogLevel.debug}))
                self.assertEqual(["Message 3"],
                                 records.containing_message("Message 3").with_levels_in({LogLevel.debug}).get_lines(timestamp=False, level=False, line_separators=False))

            records = log_context_1.pop_captured_records()
            self.assertFalse(records.containing_message("Message 1").with_levels_in({LogLevel.debug}))
            self.assertEqual(["Message 2"],
                             records.containing_message("Message 2").with_levels_in({LogLevel.debug}).get_lines(timestamp=False, level=False, line_separators=False))
            self.assertEqual(["Message 3"],
                             records.containing_message("Message 3").with_levels_in({LogLevel.debug}).get_lines(timestamp=False, level=False, line_separators=False))

    def test_log_content_nested_with_nested_capture_context_disabled(self) -> None:
        self._logger.debug("Message 1")
        with CaptureLogs() as log_context_1:
            self._logger.debug("Message 2")
            with log_context_1.nested_capture_context(shield_parent=False) as log_context_2:
                self._logger.debug("Message 3")
                records = log_context_2.pop_captured_records()
                self.assertFalse(records.containing_message("Message 1").with_levels_in({LogLevel.debug}))
                self.assertFalse(records.containing_message("Message 2").with_levels_in({LogLevel.debug}))
                self.assertEqual(["Message 3"],
                                 records.containing_message("Message 3").with_levels_in({LogLevel.debug}).get_lines(timestamp=False, level=False, line_separators=False))
            records = log_context_1.pop_captured_records()
            self.assertFalse(records.containing_message("Message 1").with_levels_in({LogLevel.debug}))
            self.assertEqual(["Message 2"],
                             records.containing_message("Message 2").with_levels_in({LogLevel.debug}).get_lines(timestamp=False, level=False, line_separators=False))
            self.assertEqual(["Message 3"],
                             records.containing_message("Message 3").with_levels_in({LogLevel.debug}).get_lines(timestamp=False, level=False, line_separators=False))

    def test_log_content_nested_with_nested_capture_context_enabled(self) -> None:
        self._logger.debug("Message 1")
        with CaptureLogs() as log_context_1:
            self._logger.debug("Message 2")
            with log_context_1.nested_capture_context(shield_parent=True) as log_context_2:
                self._logger.debug("Message 3")
                records = log_context_2.pop_captured_records()
                self.assertFalse(records.containing_message("Message 1").with_levels_in({LogLevel.debug}))
                self.assertFalse(records.containing_message("Message 2").with_levels_in({LogLevel.debug}))
                self.assertEqual(["Message 3"],
                                 records.containing_message("Message 3").with_levels_in({LogLevel.debug}).get_lines(timestamp=False, level=False, line_separators=False))
            records = log_context_1.pop_captured_records()
            self.assertFalse(records.containing_message("Message 1").with_levels_in({LogLevel.debug}))
            self.assertEqual(["Message 2"],
                             records.containing_message("Message 2").with_levels_in({LogLevel.debug}).get_lines(timestamp=False, level=False, line_separators=False))
            self.assertFalse(records.containing_message("Message 3").with_levels_in({LogLevel.debug}))

    def test_log_content_from_thread(self) -> None:
        with CaptureLogs() as log_context:
            self._run_in_thread(self._log_stuff)
            records = log_context.pop_captured_records()
            self.assertEqual(["This is debug"],
                             records.containing_message("This is debug").with_levels_in({LogLevel.debug}).get_lines(timestamp=False, level=False, line_separators=False))
            self.assertEqual(["This is warning"],
                             records.containing_message("This is warning").with_levels_in({LogLevel.warn}).get_lines(timestamp=False, level=False, line_separators=False))
            self.assertEqual(["This is error"],
                             records.containing_message("This is error").with_levels_in({LogLevel.error}).get_lines(timestamp=False, level=False, line_separators=False))

    # Logging from processes is not expected to work unless using ProcessRunner, which implements a special logging mechanism.
    # This is tested in ThreadAndProcessRunnersLoggingSlowTest.

    def test_save_captured_lines(self) -> None:
        handle, path = tempfile.mkstemp(text=True)
        os.close(handle)
        try:
            with CaptureLogs() as log_context:
                self._log_stuff()
                records = log_context.pop_captured_records()
                records.save_lines_to_file(path)

            with open(path) as f:
                lines = f.readlines()
                self.assertEqual(3, len(lines))
                self.assertTrue(lines[0].find("This is debug") >= 0)
                self.assertTrue(lines[1].find("This is warning") >= 0)
                self.assertTrue(lines[2].find("This is error") >= 0)
        finally:
            Path(path).unlink()

    @staticmethod
    def _run_in_thread(target: Callable[[], None]) -> None:
        thread = Thread(target=target)
        thread.start()
        thread.join()

    def _log_stuff(self) -> None:
        self._logger.debug("This is %s", "debug")
        self._logger.warning("This is warning")
        self._logger.error("This is error")
