import queue
from abc import ABC, abstractmethod
from threading import Thread
from time import sleep
from typing import Any, Dict, List, Tuple, cast
from unittest import TestCase

from puma.attribute import ProcessAction, ThreadAction, manually_managed
from puma.attribute.mixin import ScopedAttributesCompatibilityMixin
from puma.buffer import Buffer, MultiThreadBuffer
from puma.helpers.testing.logging.capture_logs import CaptureLogs
from puma.logging import LogLevel
from puma.runnable.proxy import Proxy
from puma.runnable.remote_execution import BaseRemoteObjectReference
from tests.runnable.proxy.proxy_test_helpers import AllMethodsReturnNone, CallResponse, HasMethodThatReturnsValue, Parent, ParentImpl, SendsCallsToBufferImpl


class ProxyCreationTest(TestCase):

    def test_ensure_an_error_raised_if_proxy_used_without_context_management(self) -> None:
        proxy = Proxy(AllMethodsReturnNone, None)
        with self.assertRaisesRegex(RuntimeError, "Must be context managed"):
            proxy.get_runner()
        with self.assertRaisesRegex(RuntimeError, "Must be context managed"):
            proxy.get_facade()

    def test_no_error_raised_if_interface_has_only_methods_that_return_none(self) -> None:
        with MultiThreadBuffer[CallResponse](10, "Test Buffer") as feedback_buffer:
            real_impl = SendsCallsToBufferImpl(feedback_buffer)
            with Proxy(AllMethodsReturnNone, real_impl) as proxy_1:
                proxy_1.get_facade()

            with Proxy(PartialProxyThatImplementsMethodWithReturnValue, real_impl) as proxy_2:
                proxy_2.get_facade()

    def test_error_raised_if_incorrect_arguments_given(self) -> None:
        with MultiThreadBuffer[CallResponse](10, "Test Buffer") as feedback_buffer:
            real_impl = SendsCallsToBufferImpl(feedback_buffer)
            with Proxy(AllMethodsReturnNone, real_impl) as proxy:
                facade = proxy.get_facade()

                with self.assertRaisesRegex(TypeError, "too many positional arguments"):
                    facade.no_args("an arg")  # type: ignore

                with self.assertRaisesRegex(TypeError, "missing a required argument: 'a'"):
                    facade.one_arg()  # type: ignore

                with self.assertRaisesRegex(TypeError, "too many positional arguments"):
                    facade.one_arg("one", "two")  # type: ignore

                with self.assertRaisesRegex(TypeError, "missing a required argument: 'a'"):
                    facade.two_args()  # type: ignore

                with self.assertRaisesRegex(TypeError, "missing a required argument: 'b'"):
                    facade.two_args("one")  # type: ignore

                with self.assertRaisesRegex(TypeError, "too many positional arguments"):
                    facade.two_args("one", "two", "three")  # type: ignore

    def test_ensure_accidentally_un_decorated_methods_work_as_expected(self) -> None:
        all_call_recorder = MethodCallRecorder(["decorated_method"])
        with Proxy(InterfaceWithOneMissingDecorator, cast(InterfaceWithOneMissingDecorator, all_call_recorder)) as proxy, \
                proxy.get_runner() as proxy_runner:
            facade = proxy.get_facade()

            proxy_runner.start_blocking()

            # Ensure calling decorated_method doesn't raise an error
            facade.decorated_method()

            with self.assertRaisesRegex(NotImplementedError, "Ooops, undecorated_method wasn't decorated"):
                facade.undecorated_method()

        # Ensure that only a call to decorated_method was requested
        commands: CallLog = all_call_recorder._call_log

        self.assertEqual(1, len(commands))
        self.assertEqual(commands["decorated_method"], ((), {}))

    def test_ensure_accidentally_un_decorated_methods_work_as_expected_in_extended_interfaces(self) -> None:
        all_call_recorder = MethodCallRecorder(["decorated_method", "decorated_extended"])
        with Proxy(ExtendedInterfaceWithMissingDecorator, cast(ExtendedInterfaceWithMissingDecorator, all_call_recorder)) as proxy, \
                proxy.get_runner() as proxy_runner:
            facade = proxy.get_facade()

            proxy_runner.start_blocking()

            # Ensure calling decorated_method doesn't raise an error
            facade.decorated_method()
            facade.decorated_extended()

            with self.assertRaisesRegex(NotImplementedError, "Ooops, undecorated_method wasn't decorated"):
                facade.undecorated_method()

            with self.assertRaisesRegex(NotImplementedError, "Ooops, undecorated_extended wasn't decorated"):
                facade.undecorated_extended()

        # Ensure that only a call to decorated_method was requested
        commands: CallLog = all_call_recorder._call_log

        self.assertEqual(2, len(commands))
        self.assertEqual(commands["decorated_method"], ((), {}))
        self.assertEqual(commands["decorated_extended"], ((), {}))

    def test_proxy_erroneously_passed_to_thread_shows_warning(self) -> None:
        with Proxy(Parent, ParentImpl()) as proxy, \
                proxy.get_runner() as proxy_runner:
            facade = proxy.get_facade()

            proxy_runner.start_blocking()

            with CaptureLogs() as log_context:
                # Retrieve a proxy of SubObject
                sub_object = facade.get_sub_object()

                # Ensure its initial state is correct
                self.assertEqual("Initial State", sub_object.get_attribute())
                # Update it's state and check that it stuck
                sub_object.set_attribute("Outer Thread State")
                self.assertEqual("Outer Thread State", sub_object.get_attribute())

                # Now, erroneously pass this Proxy object to another thread
                def thread_method() -> None:
                    # Call again in background thread. In this instance this raises an error but other side effects could occur depending on the type of the object being proxied
                    with self.assertRaisesRegex(AttributeError, "AutoRemoteObjectReference' object has no attribute '_attribute'"):
                        sub_object.get_attribute()

                # Start the thread which now erroneously has a Proxy object
                thread = Thread(target=thread_method)
                thread.start()
                thread.join()

                # Ensure outer object hasn't changed
                self.assertEqual("Outer Thread State", sub_object.get_attribute())

                # Ensure that the expected warnings were shown
                warning_logs = log_context.pop_captured_records().with_levels_in({LogLevel.warn}).get_lines(timestamp=False, level=False)
                self.assertEqual(2, len(warning_logs))  # 2 warnings, as they were raised for both "get_attribute" and "_attribute"

                def expected_log_message(attribute: str) -> str:
                    return f"No remote methods or attributes found for AutoRemoteObjectReference for <class 'tests.runnable.proxy.proxy_test_helpers.SubObjectImpl'> " \
                           f"when attempting to retrieve '{attribute}' - has it been incorrectly shared across threads?"

                self.assertEqual(expected_log_message("get_attribute"), warning_logs[0])
                self.assertEqual(expected_log_message("_attribute"), warning_logs[1])


def get_all_items_from_buffer(buffer: Buffer) -> List:
    buffer_items = []
    with buffer.subscribe(None) as sub:
        while True:
            try:
                sleep(0.01)
                sub.call_events(lambda v: buffer_items.append(v))
            except queue.Empty:
                break

    return buffer_items


CallLog = Dict[str, Tuple[Any, Any]]


class MethodCallRecorder(ScopedAttributesCompatibilityMixin):
    _call_log: CallLog = manually_managed("_call_log", ThreadAction.SHARED, ProcessAction.NOT_ALLOWED)

    def __init__(self, methods_to_record: List[str]) -> None:
        super().__init__()
        self._methods_to_record = methods_to_record
        self._call_log = {}

    def __getattribute__(self, item: str) -> Any:
        methods_to_record = super().__getattribute__("_methods_to_record")
        if item in methods_to_record:
            return CallRecorder(self._call_log, item)

        return super().__getattribute__(item)


class CallRecorder:

    def __init__(self, recording_dict: CallLog, name: str):
        self._recording_dict = recording_dict
        self._name = name

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self._recording_dict[self._name] = (args, kwargs)


class PartialProxyThatImplementsMethodWithReturnValue(BaseRemoteObjectReference[HasMethodThatReturnsValue], HasMethodThatReturnsValue, ABC):

    def returns_value(self, a: str, b: int) -> str:
        return self._remote_method(self._wrapped_instance.returns_value).call(a, b)


class InterfaceWithOneMissingDecorator(ABC):

    @abstractmethod
    def decorated_method(self) -> None:
        raise NotImplementedError

    def undecorated_method(self) -> None:
        raise NotImplementedError("Ooops, undecorated_method wasn't decorated")


class ExtendedInterfaceWithMissingDecorator(InterfaceWithOneMissingDecorator, ABC):

    @abstractmethod
    def decorated_extended(self) -> None:
        raise NotImplementedError()

    def undecorated_extended(self) -> None:
        raise NotImplementedError("Ooops, undecorated_extended wasn't decorated")
