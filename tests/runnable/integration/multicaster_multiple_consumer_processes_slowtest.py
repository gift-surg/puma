import logging
import time
from typing import TypeVar
from unittest import TestCase

from puma.buffer import MultiProcessBuffer
from puma.multicaster.multicaster import Multicaster
from puma.runnable.runner import ProcessRunner
from puma.unexpected_situation_action import UnexpectedSituationAction
from tests.runnable.test_support.publishing_runnable import PublishingRunnable
from tests.runnable.test_support.testval import TestVal
from tests.runnable.test_support.validating_single_subscription_runnable import ValidatingSingleSubscriptionRunnable
from tests.test_logging_helpers import assert_no_warnings_or_errors_logged

Type = TypeVar("Type")

logger = logging.getLogger(__name__)

BUFFER_SIZE = 10
COUNT = BUFFER_SIZE * 3
DELAY = 0.02

FAIL_TIMEOUT = 10.0
TEST_TIMEOUT = 30.0


# Test Multicaster when its outputs go to multiple processes. Most of the testing of Multicaster is done in a single thread, see
# multicaster_test.py. This 'slow' test ensures that it really works in a multi-process program.


class MulticasterMultipleConsumerProcessesSlowTest(TestCase):
    @assert_no_warnings_or_errors_logged
    def test(self) -> None:
        with MultiProcessBuffer[TestVal](BUFFER_SIZE, "Publish buffer") as publish_buffer, \
                MultiProcessBuffer[TestVal](BUFFER_SIZE, "Observer buffer 1") as subscriber_process_1_buffer, \
                MultiProcessBuffer[TestVal](BUFFER_SIZE, "Observer buffer 2") as subscriber_process_2_buffer:
            test_fail_time = time.monotonic() + TEST_TIMEOUT
            with Multicaster[TestVal](publish_buffer) as main_multicaster, \
                    ProcessRunner(PublishingRunnable(COUNT, publish_buffer, delay=DELAY), "Publisher") as publish_process, \
                    ProcessRunner(ValidatingSingleSubscriptionRunnable(subscriber_process_1_buffer, COUNT), "Consumer 1") as subscriber_process_1, \
                    ProcessRunner(ValidatingSingleSubscriptionRunnable(subscriber_process_2_buffer, COUNT), "Consumer 2") as subscriber_process_2:
                main_multicaster.subscribe(subscriber_process_1_buffer, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
                main_multicaster.subscribe(subscriber_process_2_buffer, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)

                main_multicaster.start()
                subscriber_process_1.start()
                subscriber_process_2.start()
                main_multicaster.wait_until_running(FAIL_TIMEOUT)
                subscriber_process_1.wait_until_running(FAIL_TIMEOUT)
                subscriber_process_2.wait_until_running(FAIL_TIMEOUT)
                logger.debug("Subscribers started")

                publish_process.start_blocking()  # start this only when everything else is ready
                logger.debug("Publisher started")

                while publish_process.is_alive() or subscriber_process_1.is_alive() or subscriber_process_2.is_alive():
                    main_multicaster.check_for_exceptions()
                    publish_process.check_for_exceptions()
                    subscriber_process_1.check_for_exceptions()
                    subscriber_process_2.check_for_exceptions()
                    time.sleep(0.5)
                    self.assertLess(time.monotonic(), test_fail_time, "Deadlock: Test did not end")
