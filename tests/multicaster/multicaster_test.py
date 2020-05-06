import logging
import time
from threading import Thread
from typing import List, Optional, TypeVar, no_type_check
from unittest import TestCase

from puma.buffer import MultiThreadBuffer, Observable, Publisher
from puma.helpers.testing.logging.decorator import assert_no_warnings_or_errors_logged
from puma.multicaster.multicaster import Multicaster
from puma.primitives import AutoResetEvent
from puma.unexpected_situation_action import UnexpectedSituationAction
from tests.buffer.test_support.buffer_api_test_support import TestSubscriber, receive_all

logger = logging.getLogger(__name__)

T = TypeVar("T")

OUTPUT_BUFFER_1_SIZE = 3
OUTPUT_BUFFER_2_SIZE = OUTPUT_BUFFER_1_SIZE + 2  # The output buffers have different sizes so that we can easily arrive at a situation where one is full and the other is not
INPUT_BUFFER_SIZE = OUTPUT_BUFFER_1_SIZE * 2  # Ensure input buffer has plenty of space
DELAY = 0.5
TIME_TOLERANCE = 0.3
FULL_ERROR_MESSAGE = "Full(\"Multicaster from 'Test input buffer': Unable to push to buffer 'Test output buffer 1', it is full\")"


class MulticasterTest(TestCase):
    def __init__(self, name: str) -> None:
        super().__init__(name)
        self._input_buffer = MultiThreadBuffer[str](INPUT_BUFFER_SIZE, "Test input buffer")
        self._output_buffer_1 = MultiThreadBuffer[str](OUTPUT_BUFFER_1_SIZE, "Test output buffer 1")
        self._output_buffer_2 = MultiThreadBuffer[str](OUTPUT_BUFFER_2_SIZE, "Test output buffer 2")
        self._thread: Optional[Thread] = None

    def setUp(self) -> None:
        self._input_buffer.__enter__()
        self._output_buffer_1.__enter__()
        self._output_buffer_2.__enter__()

    def tearDown(self) -> None:
        self._input_buffer.__exit__(None, None, None)
        self._output_buffer_1.__exit__(None, None, None)
        self._output_buffer_2.__exit__(None, None, None)
        # Individual tests that set self._thread should join on it
        self.assertFalse(self._thread and self._thread.is_alive())

    @assert_no_warnings_or_errors_logged
    def test_typical_case(self) -> None:
        with Multicaster(self._input_buffer) as multicaster:
            multicaster.subscribe(self._output_buffer_1, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            multicaster.subscribe(self._output_buffer_2, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            multicaster.start_blocking()
            with self._input_buffer.publish() as publisher:
                publisher.publish_value("Hello")
                publisher.publish_value("World")
                publisher.publish_complete(None)
                time.sleep(0.1)

        self._subscribe_and_receive_all_and_validate(self._output_buffer_1, values=["Hello", "World"], expect_completed=True, errors=[])
        self._subscribe_and_receive_all_and_validate(self._output_buffer_2, values=["Hello", "World"], expect_completed=True, errors=[])
        self.assertFalse(multicaster.is_alive())

    @assert_no_warnings_or_errors_logged
    def test_ok_if_no_subscribers(self) -> None:
        with Multicaster(self._input_buffer) as multicaster:
            multicaster.start_blocking()
            with self._input_buffer.publish() as publisher:
                publisher.publish_value("Hello")
                publisher.publish_complete(None)
        self.assertFalse(multicaster.is_alive())

    @assert_no_warnings_or_errors_logged
    def test_stopped_if_commanded_to_stop(self) -> None:
        with Multicaster(self._input_buffer) as multicaster:
            multicaster.subscribe(self._output_buffer_1, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            multicaster.subscribe(self._output_buffer_2, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            multicaster.start_blocking()
            with self._input_buffer.publish() as publisher:
                publisher.publish_value("Hello")
                publisher.publish_value("World")
                time.sleep(0.1)
                multicaster.stop()
                publisher.publish_value("Too late")

        self.assertFalse(multicaster.is_alive())
        self._subscribe_and_receive_all_and_validate(self._output_buffer_1, values=["Hello", "World"], expect_completed=True, errors=[])
        self._subscribe_and_receive_all_and_validate(self._output_buffer_2, values=["Hello", "World"], expect_completed=True, errors=[])

    @assert_no_warnings_or_errors_logged
    def test_stopped_if_on_complete(self) -> None:
        with Multicaster(self._input_buffer) as multicaster:
            multicaster.subscribe(self._output_buffer_1, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            multicaster.subscribe(self._output_buffer_2, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            multicaster.start_blocking()
            with self._input_buffer.publish() as publisher:
                publisher.publish_value("Hello")
                publisher.publish_value("World")
                publisher.publish_complete(None)
                time.sleep(0.1)

        self._subscribe_and_receive_all_and_validate(self._output_buffer_1, values=["Hello", "World"], expect_completed=True, errors=[])
        self._subscribe_and_receive_all_and_validate(self._output_buffer_2, values=["Hello", "World"], expect_completed=True, errors=[])
        self.assertFalse(multicaster.is_alive())

    @assert_no_warnings_or_errors_logged
    def test_on_complete_with_error(self) -> None:
        with Multicaster(self._input_buffer) as multicaster:
            multicaster.subscribe(self._output_buffer_1, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            multicaster.subscribe(self._output_buffer_2, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            multicaster.start_blocking()
            with self._input_buffer.publish() as publisher:
                publisher.publish_value("Hello")
                publisher.publish_complete(RuntimeError('Test error'))
                time.sleep(0.1)

        self._subscribe_and_receive_all_and_validate(self._output_buffer_1, values=["Hello"], expect_completed=True, errors=["RuntimeError('Test error')"])
        self._subscribe_and_receive_all_and_validate(self._output_buffer_2, values=["Hello"], expect_completed=True, errors=["RuntimeError('Test error')"])

    @assert_no_warnings_or_errors_logged
    def test_cannot_subscribe_while_running(self) -> None:
        with Multicaster(self._input_buffer) as multicaster:
            multicaster.start_blocking()
            with self.assertRaisesRegex(RuntimeError, "Output buffers may not be added or removed while the runnable is executing"):
                multicaster.subscribe(self._output_buffer_1)

    @assert_no_warnings_or_errors_logged
    def test_cannot_unsubscribe_while_running(self) -> None:
        with Multicaster(self._input_buffer) as multicaster:
            multicaster.subscribe(self._output_buffer_1)
            multicaster.start_blocking()
            with self.assertRaisesRegex(RuntimeError, "Output buffers may not be added or removed while the runnable is executing"):
                multicaster.unsubscribe(self._output_buffer_1)

    @assert_no_warnings_or_errors_logged
    def test_can_only_subscribe_a_buffer_once(self) -> None:
        with Multicaster(self._input_buffer) as multicaster:
            multicaster.subscribe(self._output_buffer_1)
            with self.assertRaisesRegex(RuntimeError, "Publishable is already subscribed"):
                multicaster.subscribe(self._output_buffer_1)

    @assert_no_warnings_or_errors_logged
    def test_can_only_unsubscribe_a_buffer_once(self) -> None:
        with Multicaster(self._input_buffer) as multicaster:
            multicaster.subscribe(self._output_buffer_1)
            multicaster.unsubscribe(self._output_buffer_1)
            with self.assertRaisesRegex(RuntimeError, "Publishable is not subscribed"):
                multicaster.unsubscribe(self._output_buffer_1)

    @assert_no_warnings_or_errors_logged
    def test_one_buffer_full_when_pushing_value_option_is_not_raising(self) -> None:
        # Testing of behaviour when output buffer fills up, when its on-full option is "ignore".
        # We expect:
        # - No errors
        # - The smaller buffer contains the data that was used to fill it
        # - The larger buffer also contains the item that was pushed, which wouldn't fit in the smaller buffer
        # - The larger buffer has been "completed" as the Multicaster has ended and the larger buffer still has room
        with Multicaster(self._input_buffer) as multicaster:
            multicaster.subscribe(self._output_buffer_1, on_full_action=UnexpectedSituationAction.IGNORE)
            multicaster.subscribe(self._output_buffer_2, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            multicaster.start_blocking()

            with self._input_buffer.publish() as publisher:
                # Fill the smaller output buffer by pushing to the multicaster's inputs. Then push one more item.
                pushed_to_fill = self._fill_output_buffer_1(publisher)
                publisher.publish_value("Hello")
                time.sleep(0.1)

        self._subscribe_and_receive_all_and_validate(self._output_buffer_1, values=pushed_to_fill, expect_completed=False, errors=[])
        self._subscribe_and_receive_all_and_validate(self._output_buffer_2, values=pushed_to_fill + ["Hello"], expect_completed=True, errors=[])
        self.assertFalse(multicaster.is_alive())

    def test_first_buffer_full_when_pushing_value_option_is_raising(self) -> None:
        # Testing of behaviour when first output buffer fills up, when its on-full option is "raise exception".
        # We expect:
        # - No errors
        # - The smaller buffer contains the data that was used to fill it
        # - The larger buffer does not contain the extra piece of data, because an error occurred
        # - The larger buffer includes on_complete, carrying the Full() error from the smaller buffer
        with Multicaster(self._input_buffer) as multicaster:
            multicaster.subscribe(self._output_buffer_1, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            multicaster.subscribe(self._output_buffer_2, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            multicaster.start_blocking()

            with self._input_buffer.publish() as publisher:
                # Fill the smaller output buffer by pushing to the multicaster's inputs. Then push one more item.
                pushed_to_fill = self._fill_output_buffer_1(publisher)
                publisher.publish_value("Extra")
                time.sleep(0.1)

        self._subscribe_and_receive_all_and_validate(self._output_buffer_1, values=pushed_to_fill, expect_completed=False, errors=[])
        self._subscribe_and_receive_all_and_validate(self._output_buffer_2, values=pushed_to_fill, expect_completed=True, errors=[FULL_ERROR_MESSAGE])
        self.assertFalse(multicaster.is_alive())

    def test_second_buffer_full_when_pushing_value_option_is_raising(self) -> None:
        # Testing of behaviour when second output buffer fills up, when its on-full option is "raise exception".
        # We expect:
        # - No errors
        # - The smaller buffer contains the data that was used to fill it
        # - The larger buffer also contains the extra piece of data, because the error happened later
        # - The larger buffer also includes on_complete, carrying the Full() error from the smaller buffer
        with Multicaster(self._input_buffer) as multicaster:
            multicaster.subscribe(self._output_buffer_2, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)  # ORDER REVERSED
            multicaster.subscribe(self._output_buffer_1, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            multicaster.start_blocking()

            with self._input_buffer.publish() as publisher:
                # Fill the smaller output buffer by pushing to the multicaster's inputs. Then push one more item.
                pushed_to_fill = self._fill_output_buffer_1(publisher)
                publisher.publish_value("Extra")
                time.sleep(0.1)

        self._subscribe_and_receive_all_and_validate(self._output_buffer_2, values=pushed_to_fill + ["Extra"], expect_completed=True, errors=[FULL_ERROR_MESSAGE])
        self._subscribe_and_receive_all_and_validate(self._output_buffer_1, values=pushed_to_fill, expect_completed=False, errors=[])
        self.assertFalse(multicaster.is_alive())

    @assert_no_warnings_or_errors_logged
    def test_one_buffer_full_when_pushing_complete_without_error_option_is_not_raising(self) -> None:
        # Same as test_one_buffer_full_when_pushing_value_option_is_not_raising, but we fill the output buffer by publishing complete (without error) rather than another value
        with Multicaster(self._input_buffer) as multicaster:
            multicaster.subscribe(self._output_buffer_1, on_full_action=UnexpectedSituationAction.IGNORE)
            multicaster.subscribe(self._output_buffer_2, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            multicaster.start_blocking()
            with self._input_buffer.publish() as publisher:
                pushed_to_fill = self._fill_output_buffer_1(publisher)
                publisher.publish_complete(None)
                time.sleep(0.1)

        self._subscribe_and_receive_all_and_validate(self._output_buffer_1, values=pushed_to_fill, expect_completed=False, errors=[])
        self._subscribe_and_receive_all_and_validate(self._output_buffer_2, values=pushed_to_fill, expect_completed=True, errors=[])
        self.assertFalse(multicaster.is_alive())

    def test_one_buffer_full_when_pushing_complete_without_error_option_is_raising(self) -> None:
        # Same as test_one_buffer_full_when_pushing_value_option_is_raising, but we fill the output buffer by publishing complete (without error) rather than sending another value
        with Multicaster(self._input_buffer) as multicaster:
            multicaster.subscribe(self._output_buffer_1, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            multicaster.subscribe(self._output_buffer_2, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            multicaster.start_blocking()
            with self._input_buffer.publish() as publisher:
                pushed_to_fill = self._fill_output_buffer_1(publisher)
                publisher.publish_complete(None)
                time.sleep(0.1)

        self._subscribe_and_receive_all_and_validate(self._output_buffer_1, values=pushed_to_fill, expect_completed=False, errors=[])
        self._subscribe_and_receive_all_and_validate(self._output_buffer_2, values=pushed_to_fill, expect_completed=True, errors=[FULL_ERROR_MESSAGE])
        self.assertFalse(multicaster.is_alive())

    @assert_no_warnings_or_errors_logged
    def test_one_buffer_full_when_pushing_complete_with_error_option_is_not_raising(self) -> None:
        # Same as test_one_buffer_full_when_pushing_value_option_is_not_raising, but we fill the output buffer by publishing complete (with error) rather than another value
        with Multicaster(self._input_buffer) as multicaster:
            multicaster.subscribe(self._output_buffer_1, on_full_action=UnexpectedSituationAction.IGNORE)
            multicaster.subscribe(self._output_buffer_2, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            multicaster.start_blocking()
            with self._input_buffer.publish() as publisher:
                pushed_to_fill = self._fill_output_buffer_1(publisher)
                publisher.publish_complete(RuntimeError('Test error'))
                time.sleep(0.1)

        self._subscribe_and_receive_all_and_validate(self._output_buffer_1, values=pushed_to_fill, expect_completed=False, errors=[])
        self._subscribe_and_receive_all_and_validate(self._output_buffer_2, values=pushed_to_fill, expect_completed=True, errors=["RuntimeError('Test error')"])
        self.assertFalse(multicaster.is_alive())

    def test_one_buffer_full_when_pushing_complete_with_error_option_is_raising(self) -> None:
        # Same as test_one_buffer_full_when_pushing_value_option_is_raising, but we fill the output buffer by publishing complete (with error) rather than sending another value
        with Multicaster(self._input_buffer) as multicaster:
            multicaster.subscribe(self._output_buffer_1, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            multicaster.subscribe(self._output_buffer_2, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)
            multicaster.start_blocking()
            with self._input_buffer.publish() as publisher:
                pushed_to_fill = self._fill_output_buffer_1(publisher)
                publisher.publish_complete(RuntimeError('Test error'))
                time.sleep(0.1)

        self._subscribe_and_receive_all_and_validate(self._output_buffer_1, values=pushed_to_fill, expect_completed=False, errors=[])
        self._subscribe_and_receive_all_and_validate(self._output_buffer_2, values=pushed_to_fill, expect_completed=True, errors=["RuntimeError('Test error')"])
        self.assertFalse(multicaster.is_alive())

    # noinspection PyTypeChecker
    @no_type_check
    def test_illegal_params(self) -> None:
        with self.assertRaises(ValueError):
            Multicaster(None)
        with Multicaster(self._input_buffer) as multicaster:
            with self.assertRaises(ValueError):
                multicaster.subscribe(None)
            with self.assertRaises(ValueError):
                multicaster.subscribe(self._output_buffer_1, None)
            with self.assertRaises(ValueError):
                multicaster.unsubscribe(None)
            # Base class methods are covered by base class tests

    @staticmethod
    def _fill_output_buffer_1(publisher: Publisher[str]) -> List[str]:
        # Pushes sufficient items to the input that the smallest output buffer will be full, once the multicaster has done its job
        pushed: List[str] = []
        for i in range(OUTPUT_BUFFER_1_SIZE):
            value = str(i)
            publisher.publish_value(value)
            pushed.append(value)
        return pushed

    def _subscribe_and_receive_all_and_validate(self, observable: Observable[str], values: List[str], expect_completed: bool, errors: List[str]) -> None:
        # Receives and checks the received values received on one of the multicaster's outputs
        subscriber = TestSubscriber()
        event = AutoResetEvent()
        with observable.subscribe(event) as subscription:
            receive_all(subscription, event, subscriber)

        subscriber.assert_published_values(values, self)
        subscriber.assert_completed(expect_completed, self)
        subscriber.assert_error_values(errors, self)
