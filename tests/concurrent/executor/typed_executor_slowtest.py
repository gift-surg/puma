from concurrent.futures import wait
from time import monotonic
from typing import Callable, Iterable, List
from unittest import TestCase

from puma.concurrent.executor.typed_executor import TypedExecutor, TypedProcessPoolExecutor, TypedThreadPoolExecutor
from puma.helpers.testing.parameterized import NamedTestParameters, parameterized
from tests.concurrent.executor.typed_executor_slowtest_methods import DELAY_SLOW, fast_method, method0, method1, method10, method2, method3, method4, method5, method6, \
    method7, method8, method9


class TypedExecutorSlowTestParams(NamedTestParameters):
    def __init__(self, executor: Callable[[], TypedExecutor]) -> None:
        super().__init__(executor.__name__)
        self._executor = executor

    @property
    def executor(self) -> TypedExecutor:
        return self._executor()


executors = [
    TypedExecutorSlowTestParams(TypedThreadPoolExecutor),
    TypedExecutorSlowTestParams(TypedProcessPoolExecutor),
]


class TypedExecutorSlowTest(TestCase):

    @parameterized(executors)
    def test_map_tuple(self, params: TypedExecutorSlowTestParams) -> None:
        with params.executor as e:
            input_params_1 = [1, 2, 3, 4, 5]
            mapped_iter_1 = e.map_tuple(method1, input_params_1)
            input_params_2 = [(1, 2), (3, 4), (5, 6), (7, 8), (9, 10)]
            mapped_iter_2 = e.map_tuple(method2, input_params_2)
            input_params_5 = [(1, 2, 3, 4, 5), (6, 7, 8, 9, 10)]
            mapped_iter_5 = e.map_tuple(method5, input_params_5)
            input_params_10 = [(1, 2, 3, 4, 5, 6, 7, 8, 9, 10), (11, 12, 13, 14, 15, 16, 17, 18, 19, 20)]
            mapped_iter_10 = e.map_tuple(method10, input_params_10)

            self._ensure_both_mapping_methods_behave_consistently(input_params_1, input_params_2, input_params_5, input_params_10,
                                                                  mapped_iter_1, mapped_iter_2, mapped_iter_5, mapped_iter_10)

    @parameterized(executors)
    def test_ensure_map_tuple_handles_different_param_sets(self, params: TypedExecutorSlowTestParams) -> None:
        with params.executor as e:
            results = e.map_tuple(fast_method, [(1,), (2, 3), (4, 5, 6)])
            results_list = list(results)
            self.assertEqual("fast_method - 1 - None - None", results_list[0])
            self.assertEqual("fast_method - 2 - 3 - None", results_list[1])
            self.assertEqual("fast_method - 4 - 5 - 6", results_list[2])

    @parameterized(executors)
    def test_map(self, params: TypedExecutorSlowTestParams) -> None:
        with params.executor as e:
            input_params_1 = [1, 2, 3, 4, 5]
            mapped_iter_1 = e.map(method1, input_params_1)
            input_params_2_1 = [1, 3, 5, 7, 9]
            input_params_2_2 = [2, 4, 6, 8, 10]
            mapped_iter_2 = e.map(method2, input_params_2_1, input_params_2_2)
            input_params_5_1 = [1, 6]
            input_params_5_2 = [2, 7]
            input_params_5_3 = [3, 8]
            input_params_5_4 = [4, 9]
            input_params_5_5 = [5, 10]
            mapped_iter_5 = e.map(method5, input_params_5_1, input_params_5_2, input_params_5_3, input_params_5_4, input_params_5_5)
            input_params_10_1 = [1, 11]
            input_params_10_2 = [2, 12]
            input_params_10_3 = [3, 13]
            input_params_10_4 = [4, 14]
            input_params_10_5 = [5, 15]
            input_params_10_6 = [6, 16]
            input_params_10_7 = [7, 17]
            input_params_10_8 = [8, 18]

            input_params_10_9 = [9, 19]
            input_params_10_10 = [10, 20]
            mapped_iter_10 = e.map(method10, input_params_10_1, input_params_10_2, input_params_10_3, input_params_10_4, input_params_10_5,
                                   input_params_10_6, input_params_10_7, input_params_10_8, input_params_10_9, input_params_10_10)

            self._ensure_both_mapping_methods_behave_consistently(input_params_1, input_params_2_1, input_params_5_1, input_params_10_1,
                                                                  mapped_iter_1, mapped_iter_2, mapped_iter_5, mapped_iter_10)

    def _ensure_both_mapping_methods_behave_consistently(self, input_params_1: List, input_params_2: List, input_params_5: List, input_params_10: List,
                                                         mapped_iter_1: Iterable[str], mapped_iter_2: Iterable[str], mapped_iter_5: Iterable[str],
                                                         mapped_iter_10: Iterable[str]) -> None:
        start = monotonic()
        mapped_1 = list(mapped_iter_1)
        mapped_2 = list(mapped_iter_2)
        mapped_5 = list(mapped_iter_5)
        mapped_10 = list(mapped_iter_10)
        duration = monotonic() - start

        # Give some wiggle room for Processes on Windows, but still ensure that methods ran in parallel
        method_call_count = len(input_params_1) + len(input_params_2) + len(input_params_5) + len(input_params_10)
        allowed_elapsed_time = (method_call_count * DELAY_SLOW) / 2
        self.assertLessEqual(duration, allowed_elapsed_time, "Futures took too long")

        self.assertEqual(5, len(mapped_1))
        self.assertEqual("method1 - 1", mapped_1[0])
        self.assertEqual("method1 - 5", mapped_1[4])

        self.assertEqual(5, len(mapped_2))
        self.assertEqual("method2 - 1 - 2", mapped_2[0])
        self.assertEqual("method2 - 9 - 10", mapped_2[4])

        self.assertEqual(2, len(mapped_5))
        self.assertEqual("method5 - 1 - 2 - 3 - 4 - 5", mapped_5[0])
        self.assertEqual("method5 - 6 - 7 - 8 - 9 - 10", mapped_5[1])

        self.assertEqual(2, len(mapped_10))
        self.assertEqual("method10 - 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9 - 10", mapped_10[0])
        self.assertEqual("method10 - 11 - 12 - 13 - 14 - 15 - 16 - 17 - 18 - 19 - 20", mapped_10[1])

    @parameterized(executors)
    def test_submit(self, params: TypedExecutorSlowTestParams) -> None:
        with params.executor as e:
            futures = [
                e.submit(method0),
                e.submit(method1, 1),
                e.submit(method2, 1, 2),
                e.submit(method3, 1, 2, 3),
                e.submit(method4, 1, 2, 3, 4),
                e.submit(method5, 1, 2, 3, 4, 5),
                e.submit(method6, 1, 2, 3, 4, 5, 6),
                e.submit(method7, 1, 2, 3, 4, 5, 6, 7),
                e.submit(method8, 1, 2, 3, 4, 5, 6, 7, 8),
                e.submit(method9, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                e.submit(method10, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
            ]

            start = monotonic()
            wait(futures)
            duration = monotonic() - start

            # Give some wiggle room for Processes on Windows, but still ensure that methods ran in parallel
            allowed_elapsed_time = (len(futures) * DELAY_SLOW) / 2
            self.assertLessEqual(duration, allowed_elapsed_time, "Futures took too long")

            self.assertEqual("method0", futures[0].result())
            self.assertEqual("method5 - 1 - 2 - 3 - 4 - 5", futures[5].result())
            self.assertEqual("method10 - 1 - 2 - 3 - 4 - 5 - 6 - 7 - 8 - 9 - 10", futures[10].result())

    @parameterized(executors)
    def test_calling_shutdown_prevents_future_scheduling(self, params: TypedExecutorSlowTestParams) -> None:
        with params.executor as e:
            submit_future = e.submit(method1, 1)

            wait([submit_future])

            e.shutdown()

            with self.assertRaisesRegex(RuntimeError, "cannot schedule new futures after shutdown"):
                e.submit(method1, 1)

            with self.assertRaisesRegex(RuntimeError, "cannot schedule new futures after shutdown"):
                e.map_tuple(method1, [1, 2, 3])
