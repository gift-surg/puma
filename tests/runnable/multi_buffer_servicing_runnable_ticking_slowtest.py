import queue
import random
import time
from dataclasses import dataclass
from threading import Thread
from typing import List, Optional
from unittest import TestCase

from puma.attribute import child_only, copied, unmanaged
from puma.buffer import MultiThreadBuffer, Observable, Publishable, Publisher, Subscriber, Subscription
from puma.helpers.testing.parameterized import parameterized
from puma.runnable import MultiBufferServicingRunnable
from puma.runnable.message import CommandMessage
from puma.runnable.runner import Runner, ThreadRunner
from tests.github_issue_11_quickfix import skip_on_windows_until_github_issue_11_is_resolved
from tests.runnable.test_support.parameterisation import RunnerTestParams, envs
from tests.test_case_helpers import assertEmpty, calc_lenient_interval_stats, calculate_intervals

INTERVAL = 0.02
MIN_COUNT = 50
BUFFER_SIZE = 10


# Tests the "ticking" functionality of MultiBufferServicingRunnable

@dataclass(frozen=True)
class ACommand(CommandMessage):
    pass


class _TickingMultiBufferServicingRunnable(MultiBufferServicingRunnable, Subscriber[int]):
    _results_publisher: Publisher[float] = child_only("_results_publisher")
    _delay_after: int = copied("_delay_after")
    tick_count: int = unmanaged("tick_count")
    pre_wait_hook_count: int = unmanaged("pre_wait_hook_count")

    def __init__(self, interval: float, input_buffer: Observable[int], results_buffer: Publishable[float],
                 *,
                 delay_after: int = -1) -> None:
        super().__init__("Test runnable", [results_buffer], tick_interval=interval)
        self._results_publisher = self._get_publisher(results_buffer)
        self._add_subscription(input_buffer, self)
        self._delay_after = delay_after
        self.tick_count = 0
        self.pre_wait_hook_count = 0

    def _on_tick(self, timestamp: float) -> None:
        self._results_publisher.publish_value(timestamp)
        self.tick_count += 1
        if self.tick_count == self._delay_after:
            assert self._tick_interval  # to keep MyPy quiet
            time.sleep(self._tick_interval * 5)

    def on_value(self, value: int) -> None:
        pass

    def on_complete(self, error: Optional[BaseException]) -> None:
        pass

    def _handle_command(self, command: CommandMessage) -> None:
        if isinstance(command, ACommand):
            pass  # Nothing to do; what matters is that receiving a command didn't interrupt the tick timing
        else:
            super()._handle_command(command)

    def _pre_wait_hook(self) -> None:
        self.pre_wait_hook_count += 1

    def _execution_ending_hook(self, error: Optional[Exception]) -> bool:
        self._results_publisher.publish_complete(error)
        return True


class MultiBufferServicingRunnableTickingSlowTest(TestCase):
    @parameterized(envs)
    def test_normal_ticking(self, param: RunnerTestParams) -> None:
        env = param._env
        with env.create_buffer(int, BUFFER_SIZE, "buffer") as in_buffer, \
                env.create_buffer(float, BUFFER_SIZE, "times") as times_buffer:
            with times_buffer.subscribe(None) as times_subscriber:
                runnable = _TickingMultiBufferServicingRunnable(INTERVAL, in_buffer, times_buffer)
                tick_times: List[float] = []
                with env.create_runner(runnable) as runner:
                    runner.start_blocking()
                    runnable.resume_ticks()
                    give_up_at = time.perf_counter() + (INTERVAL * MIN_COUNT) + 3.0
                    while (len(tick_times) < MIN_COUNT) and (time.perf_counter() < give_up_at):
                        time.sleep(0.01)
                        runner.check_for_exceptions()
                        self._transfer_items_from_times_buffer_to_list(times_subscriber, tick_times)
                self._validate_intervals(tick_times)

    @parameterized(envs)
    def test_pause_resume(self, param: RunnerTestParams) -> None:
        env = param._env
        with env.create_buffer(int, BUFFER_SIZE, "buffer") as in_buffer, \
                env.create_buffer(float, BUFFER_SIZE, "times") as times_buffer:
            with times_buffer.subscribe(None) as times_subscriber:
                runnable = _TickingMultiBufferServicingRunnable(INTERVAL, in_buffer, times_buffer)
                with env.create_runner(runnable) as runner:
                    runner.start_blocking()
                    runnable.resume_ticks()

                    tick_times = self._get_received_items_in_period(times_subscriber, INTERVAL * 10, runner)
                    self.assertTrue(tick_times)

                    runnable.pause_ticks()
                    time.sleep(0.1)
                    self._discard_queue_items(times_subscriber)

                    tick_times = self._get_received_items_in_period(times_subscriber, INTERVAL * 10, runner)
                    assertEmpty(self, tick_times)

                    runnable.resume_ticks()
                    time.sleep(0.1)
                    self._discard_queue_items(times_subscriber)

                    tick_times = self._get_received_items_in_period(times_subscriber, INTERVAL * 10, runner)
                    self.assertTrue(tick_times)

    @parameterized(envs)
    def test_change_interval_when_paused(self, param: RunnerTestParams) -> None:
        long_interval = INTERVAL * 2
        env = param._env
        with env.create_buffer(int, BUFFER_SIZE, "buffer") as in_buffer, \
                env.create_buffer(float, BUFFER_SIZE, "times") as times_buffer:
            with times_buffer.subscribe(None) as times_subscriber:
                runnable = _TickingMultiBufferServicingRunnable(INTERVAL, in_buffer, times_buffer)
                with env.create_runner(runnable) as runner:
                    runner.start_blocking()

                    # Ticking is currently paused, it will not start until resume_ticks() is called.
                    runnable.set_tick_interval(long_interval)
                    runnable.resume_ticks()

                    tick_times = self._get_received_items_in_period(times_subscriber, long_interval * 10, runner)
                    self.assertGreaterEqual(len(tick_times), 8)
                    self.assertLessEqual(len(tick_times), 12)

    @parameterized(envs)
    def test_change_interval_up_when_running(self, param: RunnerTestParams) -> None:
        long_interval = INTERVAL * 2
        env = param._env
        with env.create_buffer(int, BUFFER_SIZE, "buffer") as in_buffer, \
                env.create_buffer(float, BUFFER_SIZE, "times") as times_buffer:
            with times_buffer.subscribe(None) as times_subscriber:
                runnable = _TickingMultiBufferServicingRunnable(INTERVAL, in_buffer, times_buffer)
                with env.create_runner(runnable) as runner:
                    runner.start_blocking()
                    runnable.resume_ticks()

                    tick_times = self._get_received_items_in_period(times_subscriber, INTERVAL * 10, runner)
                    self.assertGreaterEqual(len(tick_times), 8)
                    self.assertLessEqual(len(tick_times), 12)

                    runnable.set_tick_interval(long_interval)

                    tick_times = self._get_received_items_in_period(times_subscriber, long_interval * 10, runner)
                    self.assertGreaterEqual(len(tick_times), 8)
                    self.assertLessEqual(len(tick_times), 12)

    @parameterized(envs)
    @skip_on_windows_until_github_issue_11_is_resolved
    def test_change_interval_down_when_running(self, param: RunnerTestParams) -> None:
        short_interval = INTERVAL / 2
        env = param._env
        with env.create_buffer(int, BUFFER_SIZE, "buffer") as in_buffer, \
                env.create_buffer(float, BUFFER_SIZE, "times") as times_buffer:
            with times_buffer.subscribe(None) as times_subscriber:
                runnable = _TickingMultiBufferServicingRunnable(INTERVAL, in_buffer, times_buffer)
                with env.create_runner(runnable) as runner:
                    runner.start_blocking()
                    time.sleep(3)  # Try to reduce load on CPU so that test is less likely to fail on heavily loaded machine
                    runnable.resume_ticks()

                    tick_times = self._get_received_items_in_period(times_subscriber, INTERVAL * 10, runner)
                    self.assertGreaterEqual(len(tick_times), 8)
                    self.assertLessEqual(len(tick_times), 12)

                    runnable.set_tick_interval(short_interval)
                    # Purge all ticks from Buffer
                    MultiBufferServicingRunnableTickingSlowTest._discard_queue_items(times_subscriber)

                    tick_times = self._get_received_items_in_period(times_subscriber, short_interval * 10, runner)
                    self.assertGreaterEqual(len(tick_times), 8)
                    self.assertLessEqual(len(tick_times), 12)

    @parameterized(envs)
    def test_stop_ends_ticking(self, param: RunnerTestParams) -> None:
        env = param._env
        with env.create_buffer(int, BUFFER_SIZE, "buffer") as in_buffer, \
                env.create_buffer(float, BUFFER_SIZE, "times") as times_buffer:
            with times_buffer.subscribe(None) as times_subscriber:
                runnable = _TickingMultiBufferServicingRunnable(INTERVAL, in_buffer, times_buffer)
                with env.create_runner(runnable) as runner:
                    runner.start_blocking()
                    runnable.resume_ticks()

                    tick_times = self._get_received_items_in_period(times_subscriber, INTERVAL * 10, runner)
                    self.assertTrue(tick_times)

                    runner.stop()
                    time.sleep(0.1)
                    self._discard_queue_items(times_subscriber)

                    tick_times = self._get_received_items_in_period(times_subscriber, INTERVAL * 10, runner)
                    assertEmpty(self, tick_times)

    @parameterized(envs)
    def test_commands_dont_mess_up_ticking(self, param: RunnerTestParams) -> None:
        env = param._env
        with env.create_buffer(int, BUFFER_SIZE, "buffer") as in_buffer, \
                env.create_buffer(float, BUFFER_SIZE, "times") as times_buffer:
            with times_buffer.subscribe(None) as times_subscriber:
                runnable = _TickingMultiBufferServicingRunnable(INTERVAL, in_buffer, times_buffer)
                tick_times: List[float] = []
                command_kicker_thread = Thread(target=self._randomly_send_commands,
                                               kwargs={'runnable': runnable, 'min_interval': INTERVAL / 2, 'max_interval': INTERVAL * 2, 'duration': INTERVAL * MIN_COUNT})
                with env.create_runner(runnable) as runner:
                    runner.start_blocking()
                    command_kicker_thread.start()
                    try:
                        runnable.resume_ticks()
                        give_up_at = time.perf_counter() + (INTERVAL * MIN_COUNT) + 3.0
                        while (len(tick_times) < MIN_COUNT) and (time.perf_counter() < give_up_at):
                            time.sleep(0.01)
                            runner.check_for_exceptions()
                            self._transfer_items_from_times_buffer_to_list(times_subscriber, tick_times)
                    finally:
                        command_kicker_thread.join()
                self._validate_intervals(tick_times)

    def test_input_messages_dont_mess_up_ticking(self) -> None:
        with MultiThreadBuffer[int](BUFFER_SIZE, "buffer") as in_buffer, \
                MultiThreadBuffer[float](BUFFER_SIZE, "times") as times_buffer:
            with times_buffer.subscribe(None) as times_subscriber:
                runnable = _TickingMultiBufferServicingRunnable(INTERVAL, in_buffer, times_buffer)
                tick_times: List[float] = []
                data_sender_thread = Thread(target=self._randomly_send_data,
                                            kwargs={'publishable': in_buffer, 'min_interval': INTERVAL / 2, 'max_interval': INTERVAL * 2, 'duration': INTERVAL * MIN_COUNT})
                with ThreadRunner(runnable) as runner:
                    runner.start_blocking()
                    data_sender_thread.start()
                    try:
                        runnable.resume_ticks()
                        give_up_at = time.perf_counter() + (INTERVAL * MIN_COUNT) + 3.0
                        while (len(tick_times) < MIN_COUNT) and (time.perf_counter() < give_up_at):
                            time.sleep(0.01)
                            runner.check_for_exceptions()
                            self._transfer_items_from_times_buffer_to_list(times_subscriber, tick_times)
                    finally:
                        data_sender_thread.join()
                self._validate_intervals(tick_times)

    def test_tick_behaviour_when_got_behind(self) -> None:
        # If the system is slow and misses its tick time, it should do another tick as soon as possible but shouldn't try to catch up on all the missed ticks
        with MultiThreadBuffer[int](BUFFER_SIZE, "buffer") as in_buffer, \
                MultiThreadBuffer[float](BUFFER_SIZE, "times") as times_buffer:
            with times_buffer.subscribe(None) as times_subscriber:
                runnable = _TickingMultiBufferServicingRunnable(INTERVAL, in_buffer, times_buffer, delay_after=MIN_COUNT // 2)
                tick_times: List[float] = []
                with ThreadRunner(runnable) as runner:
                    runner.start_blocking()
                    runnable.resume_ticks()
                    give_up_at = time.perf_counter() + (INTERVAL * MIN_COUNT) + 3.0
                    while (len(tick_times) < MIN_COUNT) and (time.perf_counter() < give_up_at):
                        time.sleep(0.01)
                        runner.check_for_exceptions()
                        self._transfer_items_from_times_buffer_to_list(times_subscriber, tick_times)

                intervals = calculate_intervals(tick_times)
                max_interval, mean_interval, min_interval = calc_lenient_interval_stats(intervals)
                self.assertGreaterEqual(min_interval, INTERVAL / 2)

    def test_not_in_busy_loop(self) -> None:
        # Make sure the runnable is not spinning round a tight loop but is waiting on timeouts and events
        with MultiThreadBuffer[int](BUFFER_SIZE, "buffer") as in_buffer, \
                MultiThreadBuffer[float](BUFFER_SIZE, "times") as times_buffer:
            runnable = _TickingMultiBufferServicingRunnable(INTERVAL, in_buffer, times_buffer)
            with ThreadRunner(runnable) as runner:
                runner.start_blocking()
                runnable.resume_ticks()
                give_up_at = time.perf_counter() + (INTERVAL * MIN_COUNT)
                while time.perf_counter() < give_up_at:
                    time.sleep(0.01)
                    runner.check_for_exceptions()

            self.assertGreater(runnable.tick_count, 0)
            self.assertGreater(runnable.pre_wait_hook_count, 0)
            self.assertLess(runnable.pre_wait_hook_count, runnable.tick_count + 4)
            # The set_tick_interval, resume_ticks and stop commands each produce a wake-up, and we need to allow a little leeway

    @staticmethod
    def _randomly_send_commands(runnable: _TickingMultiBufferServicingRunnable, min_interval: float, max_interval: float, duration: float) -> None:
        end_at = time.perf_counter() + duration
        while time.perf_counter() < end_at:
            time.sleep(random.uniform(min_interval, max_interval))
            runnable._send_command(ACommand())

    @staticmethod
    def _randomly_send_data(publishable: Publishable[int], min_interval: float, max_interval: float, duration: float) -> None:
        end_at = time.perf_counter() + duration
        with publishable.publish() as publisher:
            while time.perf_counter() < end_at:
                time.sleep(random.uniform(min_interval, max_interval))
                publisher.publish_value(0)

    def _validate_intervals(self, tick_times: List[float]) -> None:
        self.assertGreaterEqual(len(tick_times), MIN_COUNT)  # check we got all the items and didn't time out

        all_intervals = calculate_intervals(tick_times)

        max_interval, mean_interval, min_interval = calc_lenient_interval_stats(all_intervals)

        allowed_deviation_too_fast_ms = 0.1
        allowed_deviation_too_slow_ms = 5

        # The interval can be less than the nominal interval when one tick is late and the next is on time.
        self.assertGreaterEqual(min_interval, INTERVAL / 2 - allowed_deviation_too_fast_ms, f"Min interval too low ({min_interval}). Intervals are {all_intervals}")
        self.assertLessEqual(min_interval, INTERVAL * 3 / 2, f"Min interval too high ({min_interval}). Intervals are {all_intervals}")
        self.assertGreaterEqual(max_interval, INTERVAL / 2 - allowed_deviation_too_fast_ms, f"Max interval too low ({max_interval}). Intervals are {all_intervals}")
        self.assertLessEqual(max_interval, (INTERVAL * 3 / 2) + allowed_deviation_too_slow_ms, f"Max interval too high ({max_interval}). Intervals are {all_intervals}")
        self.assertGreaterEqual(mean_interval, INTERVAL - allowed_deviation_too_fast_ms, f"Mean interval too low ({mean_interval}). Intervals are {all_intervals}")
        self.assertLessEqual(mean_interval, INTERVAL + allowed_deviation_too_slow_ms, f"Mean interval too high ({mean_interval}). Intervals are {all_intervals}")

    @staticmethod
    def _get_received_items_in_period(times_buffer: Subscription[float], period: float, runner: Runner) -> List[float]:
        stop_at = time.perf_counter() + period
        tick_times: List[float] = []
        while time.perf_counter() < stop_at:
            time.sleep(0.01)
            runner.check_for_exceptions()
            MultiBufferServicingRunnableTickingSlowTest._transfer_items_from_times_buffer_to_list(times_buffer, tick_times)
        return tick_times

    @staticmethod
    def _transfer_items_from_times_buffer_to_list(times_buffer: Subscription[float], tick_times: List[float]) -> None:
        while True:
            try:
                times_buffer.call_events(lambda v: tick_times.append(v))
            except queue.Empty:
                break

    @staticmethod
    def _discard_queue_items(times_buffer: Subscription[float]) -> None:
        while True:
            try:
                times_buffer.call_events(lambda v: None)
            except queue.Empty:
                break

# Test it isn't waking up only to do nothing: count the pre-wait-hooks
