from typing import Any, Dict, Generic, Optional, Type, TypeVar

from puma.attribute import AccessibleScope, SCOPE_ID_PLACEHOLDER, logger
from puma.attribute.attribute.attribute_accessed_from_invalid_scope_error import AttributeAccessedFromInvalidScopeError
from puma.attribute.attribute.detected_scope import DetectedScope
from puma.attribute.attribute.instance_wrapper import InstanceWrapper
from puma.attribute.attribute.instance_wrapper_impl import InstanceWrapperImpl
from puma.attribute.attribute.scoped_attribute import SCOPE_ID_SHARED, ScopedAttribute
from puma.attribute.attribute.scoped_attribute_value_factory import ScopedAttributeValueFactory
from puma.attribute.attribute.sharing_attribute_between_scopes_not_allowed_error import SharingAttributeBetweenProcessesNotAllowedError
from puma.attribute.attribute.thread_and_process_actions import ProcessAction, ThreadAction
from puma.attribute.attribute.value_not_found_handler.use_parent_value_not_found_handler import UseParentValueNotFoundHandler
from puma.attribute.scope_id_helpers import create_attribute_id
from puma.scope_id import get_current_scope_id

T = TypeVar("T")


class ScopedAttributeImpl(Generic[T], ScopedAttribute[T]):

    def __init__(self, name: str, accessible_context: AccessibleScope, thread_action: ThreadAction, process_action: ProcessAction) -> None:
        self._name = name
        self._accessible_context = accessible_context
        self._thread_action = thread_action
        self._process_action = process_action
        self._wrapped_instance_cache: Dict[str, Optional[InstanceWrapper]] = {}
        self._cached_values: Dict[str, Optional[T]] = {}

    @property
    def name(self) -> str:
        return self._name

    @property
    def accessible_scope(self) -> AccessibleScope:
        return self._accessible_context

    def get_attribute_id(self, attribute_name: str) -> str:
        attribute_id = create_attribute_id(self.name, attribute_name)
        return f"{attribute_id}{SCOPE_ID_PLACEHOLDER}"

    @property
    def thread_action(self) -> ThreadAction:
        return self._thread_action

    @property
    def process_action(self) -> ProcessAction:
        return self._process_action

    def _get_current_scope_id(self) -> str:
        if self._thread_action == ThreadAction.SHARED:
            return SCOPE_ID_SHARED
        else:
            return get_current_scope_id()

    def _wrap_instance(self, instance: Any) -> InstanceWrapper[T]:

        current_context_id = self._get_current_scope_id()
        cache_key = self._get_cache_key(instance, current_context_id)

        cached_wrapper = self._wrapped_instance_cache.get(cache_key, None)
        if cached_wrapper:
            return cached_wrapper
        wrapper = InstanceWrapperImpl(self, instance, current_context_id)
        self._wrapped_instance_cache[cache_key] = wrapper
        return wrapper

    def _get_instance_cache_key(self, instance: Any) -> str:
        return self._get_cache_key(instance, self._get_current_scope_id())

    def _get_cache_key(self, instance: Any, current_context_id: str) -> str:
        return f"{id(instance)} - {current_context_id}"

    def __get__(self, instance: Any, owner: Type[Any]) -> T:
        if instance is None:
            return self  # type: ignore

        cache_key = self._get_instance_cache_key(instance)
        cached_value = self._cached_values.get(cache_key, None)
        if cached_value is not None:
            return cached_value

        wrapped = self._wrap_instance(instance)
        detected_context = wrapped.detect_scope()

        self._assert_accessed_from_acceptable_context(detected_context)

        # Get the attributes value
        # If the attribute has a ValueFactory and it has not yet been called, call it, store and return the value and prevent it being called again.
        # Otherwise, return its current value
        parent_value_accessor = UseParentValueNotFoundHandler(wrapped.parent_scope_id.get())

        value_factory = wrapped.value_factory.get(parent_value_accessor)
        if value_factory and not wrapped.value_factory_called.get(False):
            wrapped.value.set(value_factory.get_value(detected_context))
            wrapped.value_factory_called.set(True)

        value = wrapped.value.get(parent_value_accessor)
        self._cached_values[cache_key] = value
        return value

    def __set__(self, instance: Any, value: T) -> None:
        if instance is None:
            return

        cache_key = self._get_instance_cache_key(instance)

        wrapped = self._wrap_instance(instance)
        # Allow setting of ScopedAttributeValueFactory whatever _accessible_scope is set to,
        # as you will likely need to set a child factory from the parent scope
        if isinstance(value, ScopedAttributeValueFactory):
            if self._accessible_context == AccessibleScope.parent:
                logger.warning(f"There is no need to use a ValueFactory for parent_only accessible property '{self._name}' - you may set the value directly")
            wrapped.value_factory.set(value)
            # Ensure that this factory will be called
            wrapped.value_factory_called.set(False)
            self._cached_values[cache_key] = None
        else:
            self._assert_accessed_from_acceptable_context(wrapped.detect_scope())
            # Ensure that this set value takes precedence over the value_factory
            wrapped.value_factory_called.set(True)
            wrapped.value.set(value)
            self._cached_values[cache_key] = value

    def _assert_accessed_from_acceptable_context(self, detected_context: DetectedScope) -> None:
        if self._accessible_context == AccessibleScope.shared:
            return

        if self._accessible_context == AccessibleScope.child:
            if detected_context == DetectedScope.child:
                return
        elif self._accessible_context == AccessibleScope.parent:
            if detected_context == DetectedScope.parent:
                return

        raise AttributeAccessedFromInvalidScopeError(
            f"Attempting to access attribute '{self._name}' from invalid scope. "
            f"Access only allowed from {self._accessible_context.name.title()} but access from {detected_context.name.title()} ({detected_context.current_scope_id}) attempted"
        )

    def assert_can_be_copied_to_another_process(self) -> None:
        if self.accessible_scope != AccessibleScope.parent and self._process_action == ProcessAction.NOT_ALLOWED:
            raise SharingAttributeBetweenProcessesNotAllowedError(self._name)
