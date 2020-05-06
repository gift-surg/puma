import queue
from typing import List, Optional, TypeVar
from unittest import TestCase

from puma.buffer import Publishable, Publisher, Subscriber, Subscription
from puma.helpers.testing.mixin import NotATestCase
from puma.primitives import AutoResetEvent
from puma.timeouts import TIMEOUT_NO_WAIT
from puma.unexpected_situation_action import UnexpectedSituationAction

Type = TypeVar("Type")


class TestSubscriberBase(Subscriber[Type], NotATestCase):
    def __init__(self) -> None:
        super().__init__()
        self.completed = False
        self.published_values: List[Type] = []
        self.error_values: List[BaseException] = []

    def on_value(self, value: Type) -> None:
        self.published_values.append(value)

    def on_complete(self, error: Optional[BaseException]) -> None:
        if self.completed:
            raise RuntimeError("Received on_complete more than once")
        self.completed = True
        if error:
            self.error_values.append(error)

    def assert_published_values(self, expected_values: List[Type], test_case: TestCase) -> None:
        test_case.assertEqual(expected_values, self.published_values, "TestSubscriberBase - Assert Published Values")

    def assert_error_values(self, expected_error_representations: List[str], test_case: TestCase) -> None:
        test_case.assertEqual(expected_error_representations, list(map(repr, self.error_values)), "TestSubscriberBase - Assert Error Values")

    def assert_completed(self, expected_completion: bool, test_case: TestCase) -> None:
        test_case.assertEqual(expected_completion, self.completed, "TestSubscriberBase - Assert Completed")


TestSubscriber = TestSubscriberBase[str]


def fill_the_buffer(publisher: Publisher[str], count: int) -> None:
    for thing in range(count):
        val = str(thing)
        publisher.publish_value(val, TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)


def publish_values(publisher: Publisher[Type], values: List[Type]) -> None:
    for value in values:
        publisher.publish_value(value, TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)


def publish_complete(publisher: Publisher[Type], error: Optional[Exception]) -> None:
    publisher.publish_complete(error, TIMEOUT_NO_WAIT, on_full_action=UnexpectedSituationAction.RAISE_EXCEPTION)


def publish_values_and_complete(publishable: Publishable[Type], values: List[Type]) -> None:
    with publishable.publish() as publisher:
        publish_values(publisher, values)
        publish_complete(publisher, error=None)


def receive_all(subscription: Subscription[Type], event: Optional[AutoResetEvent], subscriber: Subscriber[Type]) -> None:
    if event:
        while True:
            if not event.wait(0.05):
                break
            while True:
                try:
                    subscription.call_events(subscriber)
                except queue.Empty:
                    break
    else:
        while True:
            try:
                subscription.call_events(subscriber)
            except queue.Empty:
                break
