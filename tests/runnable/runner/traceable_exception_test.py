import inspect
import logging
import sys
import traceback
from dataclasses import dataclass
from multiprocessing import Process
from threading import Thread
from typing import Iterable, List, Tuple, no_type_check
from unittest import TestCase

from puma.buffer import TraceableException
from puma.buffer.implementation.managed_queues import ManagedProcessQueue, ManagedQueueTypes, ManagedThreadQueue
from puma.helpers.string import safe_str
from tests.test_logging_helpers import assert_no_warnings_or_errors_logged

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _QueuedInfo:
    traceable_exception: TraceableException
    line_a: int


class TraceableExceptionTest(TestCase):
    def test_formatting(self) -> None:
        try:
            raise RuntimeError("Test123")
        except Exception as ex:
            traceable_exception = TraceableException(ex)

        self.assertEqual("TraceableException(RuntimeError('Test123'))", safe_str(traceable_exception))

    @assert_no_warnings_or_errors_logged
    def test_traceback_single_threaded(self) -> None:
        traceable_exception, line_a = self._create_traceable_exception()
        lines, line_b = self._re_raise_runner_error(traceable_exception)
        self._verify_exception_stack(line_a, line_b, lines)

    @assert_no_warnings_or_errors_logged
    def test_traceback_multiple_stages_single_threaded_1(self) -> None:
        traceable_exception_1, line_a = self._create_traceable_exception()
        error_1 = traceable_exception_1.get_error()
        traceable_exception_2 = TraceableException(error_1)
        error_2 = traceable_exception_2.get_error()
        traceable_exception_3 = TraceableException(error_2)
        lines, line_b = self._re_raise_runner_error(traceable_exception_3)
        self._verify_exception_stack(line_a, line_b, lines)

    @assert_no_warnings_or_errors_logged
    def test_traceback_multiple_stages_single_threaded_2(self) -> None:
        traceable_exception_1, line_a = self._create_traceable_exception()
        error_1 = traceable_exception_1.get_error()
        traceable_exception_2 = TraceableException(error_1)
        error_2 = traceable_exception_2.get_error()
        lines, line_b = self._raise_error(error_2)
        self._verify_exception_stack(line_a, line_b, lines)

    @assert_no_warnings_or_errors_logged
    def test_traceback_multi_threaded(self) -> None:
        with ManagedThreadQueue[_QueuedInfo]() as queue:
            thread = Thread(name="raise_traceable_exception", target=self._push_traceable_exception, args=(queue,))
            thread.start()
            thread.join(5.0)
            raised: _QueuedInfo = queue.get_nowait()
            lines, line_b = self._re_raise_runner_error(raised.traceable_exception)
            self._verify_exception_stack(raised.line_a, line_b, lines)

    @assert_no_warnings_or_errors_logged
    def test_traceback_multi_process(self) -> None:
        with ManagedProcessQueue[_QueuedInfo]() as queue:
            process = Process(name="raise_traceable_exception", target=self._push_traceable_exception, args=(queue,))
            process.start()
            process.join(5.0)
            raised: _QueuedInfo = queue.get_nowait()
            lines, line_b = self._re_raise_runner_error(raised.traceable_exception)
            self._verify_exception_stack(raised.line_a, line_b, lines)

    # noinspection PyTypeChecker
    @no_type_check
    def test_illegal_params(self) -> None:
        with self.assertRaises(ValueError):
            TraceableException(None)

    @staticmethod
    def _push_traceable_exception(queue: ManagedQueueTypes[_QueuedInfo]) -> None:
        queued = _QueuedInfo(*TraceableExceptionTest._create_traceable_exception())
        queue.put(queued)

    def _verify_exception_stack(self, line_a: int, line_b: int, lines: List[str]) -> None:
        tidied = []
        for line in lines:
            tidied.extend([subline.strip() for subline in line.splitlines()])
        self._assert_contains_substring(f'line {line_a}', tidied)
        self._assert_contains_substring('raise RuntimeError("Test123")', tidied)
        self._assert_contains_substring(f'line {line_b}', tidied)

    @staticmethod
    def _re_raise_runner_error(traceable_exception: TraceableException) -> Tuple[List[str], int]:
        return TraceableExceptionTest._raise_error(traceable_exception.get_error())

    @staticmethod
    def _raise_error(exception: BaseException) -> Tuple[List[str], int]:
        line_b = 0

        try:
            line_b = TraceableExceptionTest._lineno() + 1
            raise exception
        except RuntimeError:
            lines = traceback.format_exception(*sys.exc_info())
        return lines, line_b

    @staticmethod
    def _create_traceable_exception() -> Tuple[TraceableException, int]:
        line_a = 0
        try:
            line_a = TraceableExceptionTest._lineno() + 1
            raise RuntimeError("Test123")
        except Exception as ex:
            traceable_exception = TraceableException(ex)
            return traceable_exception, line_a

    def _assert_contains_substring(self, substring: str, lines: Iterable[str]) -> None:
        self.assertTrue(self._contains_substring(substring, lines), f"Failed to find substring '{substring}' in reported lines: {lines}")

    @staticmethod
    def _contains_substring(substring: str, lines: Iterable[str]) -> bool:
        for line in lines:
            if substring in line:
                return True
        return False

    @staticmethod
    def _lineno() -> int:
        """Returns the current line number in our program."""
        frame = inspect.currentframe()
        if not frame:
            return 0
        return frame.f_back.f_lineno
