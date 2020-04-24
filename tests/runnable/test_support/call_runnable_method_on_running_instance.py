from typing import Any, Callable
from unittest import TestCase

from puma.runnable import Runnable
from puma.runnable.runner import ThreadRunner


def call_runnable_method_on_running_instance(test_case: TestCase, runnable_factory: Callable[[], Runnable], test_callback: Callable[[Runnable], Any]) -> None:
    runnable = runnable_factory()
    with ThreadRunner(runnable) as runner:
        runner.start_blocking()
        with test_case.assertRaises(ValueError):
            test_callback(runnable)
