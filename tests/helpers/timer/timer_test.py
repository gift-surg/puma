from time import sleep
from unittest import TestCase

from puma.helpers.timer import Timer


class TimerTest(TestCase):

    def test_get_elapsed_time_works(self) -> None:
        with Timer() as timer:
            sleep(0.4)
            self.assertAlmostEqual(timer.get_elapsed_time(), 0.4, delta=0.05)

    def test_sub_timer_and_get_elapsed_time_works(self) -> None:
        with Timer() as timer_1:
            sleep(0.4)
            with timer_1.sub_timer() as timer_2:
                sleep(0.2)
                self.assertAlmostEqual(timer_2.get_elapsed_time(), 0.2, delta=0.05)
                with timer_2.sub_timer() as timer_3:
                    sleep(0.3)
                    self.assertAlmostEqual(timer_3.get_elapsed_time(), 0.3, delta=0.05)
                self.assertAlmostEqual(timer_2.get_elapsed_time(), 0.5, delta=0.05)
            self.assertAlmostEqual(timer_1.get_elapsed_time(), 0.9, delta=0.05)
