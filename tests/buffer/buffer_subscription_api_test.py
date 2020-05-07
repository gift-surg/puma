import logging
import queue
from typing import no_type_check
from unittest import TestCase

from puma.buffer import Buffer
from puma.helpers.testing.logging.decorator import assert_no_warnings_or_errors_logged
from puma.helpers.testing.parameterized import parameterized
from puma.primitives import AutoResetEvent
from tests.buffer._parameterisation import BufferTestEnvironment, BufferTestParams, envs
from tests.buffer.test_support.buffer_api_test_support import TestSubscriber, publish_values_and_complete

logger = logging.getLogger(__name__)

BUFFER_SIZE = 10
TIMEOUT = 0.3


class BufferSubscriptionApiTest(TestCase):

    def setUp(self) -> None:
        self._subscriber1: TestSubscriber = TestSubscriber()

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_pop_using_subscriber(self, param: BufferTestParams) -> None:
        items = ["#1", "#2", "#3"]
        env = param._env
        event = AutoResetEvent()
        with self._create_buffer(env) as buffer:
            with buffer.subscribe(event) as subscription:
                publish_values_and_complete(buffer, items)
                env.publish_observe_delay()
                while True:
                    if not event.wait(TIMEOUT):
                        break
                    while True:
                        try:
                            subscription.call_events(self._subscriber1)
                        except queue.Empty:
                            break
                self._subscriber1.assert_published_values(items, self)
                self._subscriber1.assert_completed(True, self)
                self._subscriber1.assert_error_values([], self)

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_pop_using_method_calls(self, param: BufferTestParams) -> None:
        items = ["#1", "#2", "#3"]
        env = param._env
        event = AutoResetEvent()
        with self._create_buffer(env) as buffer:
            with buffer.subscribe(event) as subscription:
                publish_values_and_complete(buffer, items)
                env.publish_observe_delay()
                while True:
                    if not event.wait(TIMEOUT):
                        break
                    while True:
                        try:
                            subscription.call_events(self._subscriber1.on_value, self._subscriber1.on_complete)
                        except queue.Empty:
                            break
                self._subscriber1.assert_published_values(items, self)
                self._subscriber1.assert_completed(True, self)
                self._subscriber1.assert_error_values([], self)

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_pop_using_method_calls_without_on_complete(self, param: BufferTestParams) -> None:
        items = ["#1", "#2", "#3"]
        env = param._env

        event = AutoResetEvent()
        with self._create_buffer(env) as buffer:
            with buffer.subscribe(event) as subscription:
                publish_values_and_complete(buffer, items)
                env.publish_observe_delay()
                while True:
                    if not event.wait(TIMEOUT):
                        break
                    while True:
                        try:
                            subscription.call_events(self._subscriber1.on_value, None)
                        except queue.Empty:
                            break
                self._subscriber1.assert_published_values(items, self)
                self._subscriber1.assert_completed(False, self)
                self._subscriber1.assert_error_values([], self)

    # noinspection PyTypeChecker
    @parameterized(envs)
    @no_type_check
    def test_illegal_params(self, param: BufferTestParams) -> None:
        env = param._env

        with self._create_buffer(env) as buffer:
            with buffer.subscribe(None) as subscription:
                with self.assertRaisesRegex(TypeError, "on_value_or_subscriber must not be None"):
                    subscription.call_events(None)
                with self.assertRaisesRegex(TypeError, "on_value_or_subscriber is not of the correct type"):
                    subscription.call_events(12345)

    @staticmethod
    def _create_buffer(env: BufferTestEnvironment) -> Buffer[str]:
        return env.create_buffer(str, BUFFER_SIZE, "buffer")
