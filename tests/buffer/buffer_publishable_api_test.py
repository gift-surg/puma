from contextlib import ExitStack
from typing import no_type_check
from unittest import TestCase

from puma.helpers.testing.logging.decorator import assert_no_warnings_or_errors_logged
from puma.helpers.testing.parameterized import parameterized
from puma.logging import LogLevel
from tests.buffer._parameterisation import BufferTestParams, envs
from tests.buffer.test_support.buffer_api_test_support import TestSubscriber

BUFFER_SIZE = 10
COUNT = BUFFER_SIZE - 1


class BufferPublishableApiTest(TestCase):
    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_buffer_name(self, param: BufferTestParams) -> None:
        env = param._env
        with env.create_buffer(str, BUFFER_SIZE, "abuff_name") as buf:
            self.assertEqual("abuff_name", buf.buffer_name())

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_single_publish(self, param: BufferTestParams) -> None:
        env = param._env
        with env.create_buffer(str, BUFFER_SIZE, "buffer") as buffer:
            with buffer.publish() as publisher:
                for i in range(COUNT):
                    publisher.publish_value(str(i))
                publisher.publish_complete(None)

                subscriber = TestSubscriber()
                with buffer.subscribe(None) as subscription:
                    env.publish_observe_delay()
                    while not subscriber.completed:
                        subscription.call_events(subscriber)

                subscriber.assert_published_values([str(i) for i in range(COUNT)], self)
                subscriber.assert_error_values([], self)

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_multiple_publishes_sequentially(self, param: BufferTestParams) -> None:
        env = param._env
        with env.create_buffer(str, BUFFER_SIZE, "buffer") as buffer:
            with buffer.subscribe(None) as subscription:
                for i in range(COUNT):
                    with buffer.publish() as publisher:
                        publisher.publish_value(str(i))
                with buffer.publish() as publisher:
                    publisher.publish_complete(None)

                env.publish_observe_delay()
                subscriber = TestSubscriber()
                while not subscriber.completed:
                    subscription.call_events(subscriber)

                subscriber.assert_published_values([str(i) for i in range(COUNT)], self)
                subscriber.assert_error_values([], self)

    @parameterized(envs)
    @assert_no_warnings_or_errors_logged
    def test_multiple_publishes_simultaneously(self, param: BufferTestParams) -> None:
        env = param._env
        with env.create_buffer(str, BUFFER_SIZE, "buffer") as buffer:
            with buffer.subscribe(None) as subscription:
                with ExitStack() as exit_stack:
                    publishers = []
                    for i in range(COUNT + 1):
                        publisher = buffer.publish()
                        publishers.append(exit_stack.enter_context(publisher))

                    for i in range(COUNT):
                        publishers[i].publish_value(str(i))
                    publishers[COUNT].publish_complete(None)

                env.publish_observe_delay()
                subscriber = TestSubscriber()
                while not subscriber.completed:
                    subscription.call_events(subscriber)

                subscriber.assert_published_values([str(i) for i in range(COUNT)], self)
                subscriber.assert_error_values([], self)

    @parameterized(envs)
    def test_too_many_unpublishes(self, param: BufferTestParams) -> None:
        with assert_no_warnings_or_errors_logged(self) as log_context_outer:
            env = param._env
            with env.create_buffer(str, BUFFER_SIZE, "buffer") as buffer:
                publisher = buffer.publish()
                publisher.__enter__()
                publisher.__exit__(None, None, None)  # calls buffer.unpublish
                with log_context_outer.nested_capture_context(LogLevel.warn, shield_parent=True) as log_context:
                    buffer.unpublish(publisher)
                    records = log_context.pop_captured_records()
                    self.assertTrue(records.containing_message("Ignoring buffer unpublish, not published").with_levels_in({LogLevel.warn}))

    @parameterized(envs)
    def test_destroy_while_still_published(self, param: BufferTestParams) -> None:
        with assert_no_warnings_or_errors_logged(self) as log_context_outer:
            env = param._env
            buf = env.create_buffer(str, BUFFER_SIZE, "buffer")
            buffer = buf.__enter__()
            buffer.publish()
            with log_context_outer.nested_capture_context(LogLevel.warn, shield_parent=True) as log_context:
                buf.__exit__(None, None, None)
                records = log_context.pop_captured_records()
                self.assertTrue(records.containing_message("Buffer being destroyed while still published to").with_levels_in({LogLevel.warn}))

    # noinspection PyTypeChecker
    @parameterized(envs)
    @no_type_check
    def test_illegal_params(self, param: BufferTestParams) -> None:
        env = param._env
        with env.create_buffer(str, BUFFER_SIZE, "buffer") as buffer:
            with self.assertRaisesRegex(ValueError, "Unpublish: publisher must not be None"):
                buffer.unpublish(None)
