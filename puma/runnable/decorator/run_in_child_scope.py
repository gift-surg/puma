import functools
import inspect
from functools import update_wrapper
from typing import Any, Callable, Type, TypeVar, cast
from uuid import uuid4

from puma.helpers.assert_set import assert_set
from puma.runnable import Runnable
from puma.runnable.message import RemoteObjectMethodCommandMessage, RunInChildScopeStatusMessage, status_message_type
from puma.runnable.remote_execution import RemoteObjectResultReference, RunnableRemoteObjectStatusBufferSubscriptionManager

RUN_IN_CHILD_CALL_TIMEOUT = 30

DecoratedFunction = Callable[..., Any]


class RunInChildScope:
    """
    Decorator class that wraps a method allowing it to be called synchronously in the Runnable's child scope (ie, background thread or process)
    Note: If the decorated method returns a non-primitive object, a RemoteObjectResultReference (ie a Proxy of the resultant value) is returned instead.
          This may incur performance penalties if its methods are called / attributes accessed too often. Currently, a warning is shown in these circumstances
    """

    def __init__(self, method: DecoratedFunction) -> None:
        self._method = method

    # TODO: Improve / fix the type hinting here - requires MyPy to add support: https://github.com/python/mypy/issues/3157
    def __get__(self, instance: Runnable, owner: Type[Runnable]) -> DecoratedFunction:
        """Returns the scoped method selection function with the correct value of 'self' bound to it"""
        partial_func = functools.partial(self._call_in_correct_scope, instance)
        update_wrapper(partial_func, self._method)
        return partial_func

    def _call_in_correct_scope(self, instance_method_self: Runnable, *args: Any, **kwargs: Any) -> Any:
        """Calls the correct method based on the current scope; either _call_in_parent_scope and _call_in_child_scope"""

        # Ensure the method belongs to a Runnable
        if not isinstance(instance_method_self, Runnable):
            raise TypeError("run_in_child_scope decorator may only be used on methods within a Runnable")

        # Ensure that the Runnable has a command buffer so that the command message can be sent
        if not instance_method_self.run_in_child_scope_accessor.has_command_buffer():
            raise RuntimeError("run_in_child_scope decorated methods may only be called once the Runnable has been started")

        if instance_method_self.run_in_child_scope_accessor.is_in_child_scope():
            return self._call_in_child_scope(*[instance_method_self, *args], **kwargs)
        else:
            return self._call_in_parent_scope(*[instance_method_self, *args], **kwargs)

    def _call_in_parent_scope(self, instance_method_self: Runnable, *args: Any, **kwargs: Any) -> Any:
        """Handles delegating the called method to the child scope via the Command Buffer"""

        # Ensure the method has been called with the correct arguments
        #    This ensures that an error is raised with a sensible traceback if the method is called with the wrong arguments
        inspect.signature(self._method).bind(*[instance_method_self, *args], **kwargs)

        # Create a unique ID for this call
        call_id = self._generate_call_id()

        # Send a command to the child scope
        instance_method_self.run_in_child_scope_accessor.send_command(RemoteObjectMethodCommandMessage(call_id, self._method.__name__, None, args, kwargs))

        return self._wait_for_remote_call_to_complete(instance_method_self, call_id)

    def _wait_for_remote_call_to_complete(self, instance_method_self: Runnable, call_id: str) -> Any:

        # Wait for the specific post-call status message to arrive
        return_message_type = status_message_type(call_id, RunInChildScopeStatusMessage)

        try:
            remote_result = instance_method_self.wait_for_status_message(return_message_type, RUN_IN_CHILD_CALL_TIMEOUT)
        except TimeoutError as te:
            raise TimeoutError("run_in_child_scope method took too long to respond - is the background Thread / Process too busy? "
                               "Consider checking _on_tick or subscriber on_value methods") from te

        if isinstance(remote_result.result, RemoteObjectResultReference):
            if instance_method_self.run_in_child_scope_accessor.get_command_message_buffer():
                return remote_result.result.with_attached_remote_buffers(instance_method_self._get_command_message_buffer(),
                                                                         RunnableRemoteObjectStatusBufferSubscriptionManager(assert_set(instance_method_self._status_subscription)))

        return remote_result.result

    def _generate_call_id(self) -> str:
        return str(uuid4())

    def _call_in_child_scope(self, instance_method_self: Runnable, *args: Any, **kwargs: Any) -> Any:
        """Handles calling the delegated method in the child scope"""
        return self._method(instance_method_self, *args, **kwargs)


# Wrap RunInChildScope so that it returns a function with the same signature as the original method
DecoratedFunctionType = TypeVar("DecoratedFunctionType", bound=DecoratedFunction)


def run_in_child_scope(function: DecoratedFunctionType) -> DecoratedFunctionType:
    return cast(DecoratedFunctionType, RunInChildScope(function))
