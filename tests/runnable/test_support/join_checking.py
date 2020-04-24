import logging
from multiprocessing import Process
from threading import Thread
from typing import Union
from unittest import TestCase

from puma.runnable.runner import Runner

logger = logging.getLogger(__name__)

FAIL_TIMEOUT = 30.0


def join_checking(thread_or_process: Union[Thread, Process, Runner], thread_or_process_description: str, testcase: TestCase) -> None:
    logger.debug("Waiting for %s to end", thread_or_process_description)
    thread_or_process.join(FAIL_TIMEOUT)
    if thread_or_process.is_alive():
        testcase.fail(f"Deadlock: {thread_or_process_description} did not end")
    else:
        logger.debug("%s ended", thread_or_process_description)
