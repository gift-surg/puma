from unittest import TestCase

from puma.attribute.attribute.sharing_attribute_between_scopes_not_allowed_error import SharingAttributeBetweenProcessesNotAllowedError, \
    SharingAttributeBetweenThreadsNotAllowedError
from puma.environment import ProcessEnvironment, ThreadEnvironment
from puma.runnable.remote_execution import CannotSetRemoteAttributeError
from tests.attribute.manually_managed import InvalidParams, ProcessCopiedTestRunnable, ProcessNotAllowedTestRunnable, ProcessSetToNoneTestRunnable, ThreadCopiedTestRunnable, \
    ThreadNotAllowedTestRunnable, ThreadSharedTestRunnable, ValidParams
from tests.parameterized import parameterized

valid_runnables = [
    ValidParams(ThreadEnvironment(), ThreadSharedTestRunnable(), 510),
    ValidParams(ThreadEnvironment(), ThreadCopiedTestRunnable(), 505),
    ValidParams(ProcessEnvironment(), ProcessCopiedTestRunnable(), 505),
    ValidParams(ProcessEnvironment(), ProcessSetToNoneTestRunnable(), 505, 105)
]


class ScopedAttributesManuallyManagedSlowTest(TestCase):

    @parameterized(valid_runnables)
    def test_valid_runnables(self, param: ValidParams) -> None:
        runnable = param.runnable
        with param.environment.create_runner(runnable) as runner:
            runner.start_blocking()

            for i in range(5):
                runnable.increment_value()
                runnable.count += 1
                runnable.sub_object.attr += 1

            self.assertEqual(param.expected_context_count_local, runnable.count)
            self.assertEqual(param.expected_context_count_local, runnable.sub_object.attr)
            remote_value = runnable.get_remote_counter_value()
            self.assertEqual(param.expected_context_count_remote, remote_value.count)
            self.assertEqual(param.expected_context_count_remote, remote_value.sub_object)

            with self.assertRaisesRegex(CannotSetRemoteAttributeError, "Cannot set attribute 'count' via RemoteObjectReference"):
                remote_value.count = 12345  # type: ignore

    def test_invalid_runnables_threads(self) -> None:
        self._test_invalid_runnables(InvalidParams(ThreadEnvironment(), ThreadNotAllowedTestRunnable(), SharingAttributeBetweenThreadsNotAllowedError))

    def test_invalid_runnables_processes(self) -> None:
        self._test_invalid_runnables(InvalidParams(ProcessEnvironment(), ProcessNotAllowedTestRunnable(), SharingAttributeBetweenProcessesNotAllowedError))

    def _test_invalid_runnables(self, param: InvalidParams) -> None:
        runnable = param.runnable
        with self.assertRaisesRegex(param.expected_error_type, "Attribute 'count' may not be passed between (Threads|Processes) "
                                                               "as its (ThreadAction|ProcessAction) is 'NOT_ALLOWED'"):
            with param.environment.create_runner(runnable) as runner:
                runner.start_blocking()
