import logging
import queue
from logging import LogRecord
from typing import Any, List, Optional, Set, Sized

from puma.context import Exit_1, Exit_2, Exit_3
from puma.logging import LogLevel, Logging, ManagedProcessLogQueue


class CapturedRecords(Sized):
    """Represents the log records captured by CaptureLogContext."""

    def __init__(self, records: List[LogRecord]) -> None:
        self._records = records

    def with_levels_in(self, levels_of_interest: Set[LogLevel]) -> 'CapturedRecords':
        """Returns a filtered copy of the records collection, containing only those records whose log level is one of the given levels."""
        levels = self._log_levels_to_ints(levels_of_interest)
        return CapturedRecords([log_record for log_record in self._records if log_record.levelno in levels])

    def containing_message(self, substring: str) -> 'CapturedRecords':
        """Returns a filtered copy of the records collection, containing only those records whose message contains the given substring."""
        return CapturedRecords([log_record for log_record in self._records if log_record.getMessage().find(substring) >= 0])

    def from_module(self, module: str) -> 'CapturedRecords':
        """Returns a filtered copy of the records collection, containing only those records originating from the given module (e.g. 'capture_logs')."""
        return CapturedRecords([log_record for log_record in self._records if log_record.module == module])

    def from_package(self, package: str) -> 'CapturedRecords':
        """Returns a filtered copy of the records collection, containing only those records originating from the given package (e.g. 'puma' or 'puma_test.context')."""

        return CapturedRecords([log_record for log_record in self._records if log_record.name.startswith(f"{package}.")])

    def __len__(self) -> int:
        """Returns the number of log records."""
        return len(self._records)

    def __bool__(self) -> bool:
        """Returns True if there are any records, False if there are none."""
        return bool(self._records)

    def get_lines(self, *, prefix: str = "", timestamp: bool = True, level: bool = True, line_separators: bool = False) -> List[str]:
        """Returns the captured records, formatted into human-readable form."""
        return [self._format_log_record(log_record, prefix, timestamp, level, line_separators) for log_record in self._records]

    def print_lines(self, *, prefix: str = "", timestamp: bool = True, level: bool = True) -> None:
        """Prints log records to stdout. Optionally, each line can be preceded with a prefix to make it stand out."""
        for log_record in self._records:
            print(self._format_log_record(log_record, prefix, timestamp, level, line_separators=True))

    def save_lines_to_file(self, filename: str, *, timestamp: bool = True, level: bool = True) -> None:
        """Saves log records to a text file."""
        with open(filename, "w") as file:
            file.writelines(self._format_log_record(log_record, prefix='', timestamp=timestamp, level=level, line_separators=True) for log_record in self._records)

    @staticmethod
    def _format_log_record(log_record: Any, prefix: str, timestamp: bool, level: bool, line_separators: bool) -> str:
        ret: str = prefix
        if timestamp:
            ret += f"{log_record.created:.5f} "
        if level:
            ret += f"{logging.getLevelName(log_record.levelno)} "
        if timestamp or level:
            ret += '- '
        ret += log_record.getMessage()
        if line_separators:
            ret += '\n'
        return ret

    @staticmethod
    def _log_levels_to_ints(levels_of_interest: Set[LogLevel]) -> Set[int]:
        return {level.value for level in levels_of_interest}


class CaptureLogContext:
    """Class returned by CaptureLogs context management, starts a new capture session and provides accessors onto the captured data."""

    def __init__(self, level: LogLevel, parent_to_shield: Optional['CaptureLogContext']) -> None:
        self._capture_queue: ManagedProcessLogQueue = ManagedProcessLogQueue(name='log capture')
        # Needs to be a process queue so we can capture logs from processes launched during the capture

        self._capture_queue.__enter__()
        self._handler_name = Logging.add_memory_log_handler(self._capture_queue, level)
        self._parent_to_shield = parent_to_shield
        if self._parent_to_shield:
            self._parent_old_level = Logging.pause_memory_log_handler(self._parent_to_shield._handler_name)

    def close(self) -> None:
        Logging.remove_memory_log_handler(self._handler_name)
        self._capture_queue.__exit__(None, None, None)
        if self._parent_to_shield and self._parent_old_level is not None:
            Logging.resume_memory_log_handler(self._parent_to_shield._handler_name, self._parent_old_level)

    def pop_captured_records(self) -> CapturedRecords:
        """Pops all log records captured thus far."""
        ret_list: List[LogRecord] = []
        while True:
            try:
                # Because we use a multiprocess buffer to capture the logs, we need to allow a little time for pushed records to be available to pop
                ret_list.append(self._capture_queue.get(timeout=0.1))
            except queue.Empty:
                return CapturedRecords(ret_list)

    def nested_capture_context(self, level: LogLevel = LogLevel.debug, *, shield_parent: bool = True) -> 'CaptureLogs':
        """Creates a nested (child) logging context that by default gobbles up all log events, preventing them reaching this context"""
        return CaptureLogs(level, self if shield_parent else None)


class CaptureLogs:
    """Represents a new context for capturing logs for testing. Captures log messages that would be output by any configured logger as currently configured."""

    def __init__(self, level: LogLevel = LogLevel.debug, parent_to_shield: Optional['CaptureLogContext'] = None):
        """Constructor. The parent_to_shield should not be used by application code - use CaptureLogContext.nested_capture_context()"""
        self._level = level
        self._parent_to_shield = parent_to_shield

    def __enter__(self) -> CaptureLogContext:
        self._context = CaptureLogContext(self._level, self._parent_to_shield)
        return self._context

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        self._context.close()
