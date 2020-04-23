import logging
import types
from dataclasses import Field
from typing import Any, Dict, Generic, List, Optional, Tuple, Type, TypeVar

from puma.attribute import ATTRIBUTE_NAME_PREFIX, copied, factory
from puma.buffer import Buffer
from puma.runnable.message import CommandMessage
from puma.runnable.remote_execution import BaseRemoteObjectReference, RemoteObjectAttribute, RemoteObjectMethod, RemoteObjectStatusBufferSubscriptionManager

logger = logging.getLogger(__name__)

T = TypeVar("T")


class AutoRemoteObjectReference(Generic[T], BaseRemoteObjectReference[T]):
    _interface: Type = copied("_interface")
    _remote_methods: Dict[str, types.MethodType] = copied("_remote_methods")
    _remote_attributes: Dict[str, RemoteObjectAttribute] = copied("_remote_attributes")

    def __init__(self, interface: Type, wrapped: T, remote_object_command_buffer: Buffer[CommandMessage],
                 remote_object_status_buffer_subscription_manager: RemoteObjectStatusBufferSubscriptionManager) -> None:
        super().__init__(wrapped, remote_object_command_buffer, remote_object_status_buffer_subscription_manager)
        self._interface = interface
        self._remote_methods = factory(self._generate_remote_methods)
        self._remote_attributes = factory(self._generate_remote_attributes)

    def _get_bind_object(self) -> Optional[str]:
        return self._wrapped_instance if isinstance(self._wrapped_instance, str) else None

    def _get_all_public_interface_attributes_and_names(self) -> List[Tuple[str, Any]]:
        # Ignore "protected (prefixed with '_')" or "private (prefixed with '__')" methods and attributes
        return [(name, getattr(self._interface, name)) for name in dir(self._interface) if not name.startswith(("_", "__"))]

    def _generate_remote_methods(self) -> Dict[str, types.MethodType]:
        remote_methods = {}
        bind_object = self._get_bind_object()
        for name, item in self._get_all_public_interface_attributes_and_names():
            if callable(item):
                if bind_object or getattr(item, "__isabstractmethod__", None):
                    remote_method = RemoteObjectMethod(bind_object,
                                                       item,
                                                       self._remote_object_command_buffer,
                                                       self._remote_object_status_buffer_subscription_manager)
                    remote_methods[name] = types.MethodType(remote_method, self)

        return remote_methods

    def _generate_remote_attributes(self) -> Dict[str, RemoteObjectAttribute]:
        remote_attributes = {}
        bind_object = self._get_bind_object()
        for name, item in self._get_all_public_interface_attributes_and_names():
            if not callable(item):
                if bind_object:
                    remote_attributes[name] = RemoteObjectAttribute(bind_object,
                                                                    name,
                                                                    self._remote_object_command_buffer,
                                                                    self._remote_object_status_buffer_subscription_manager)

        dataclass_fields: Dict[str, Field] = getattr(self._interface, "__dataclass_fields__", {})
        for data_class_field in dataclass_fields.keys():
            remote_attributes[data_class_field] = RemoteObjectAttribute(bind_object,
                                                                        data_class_field,
                                                                        self._remote_object_command_buffer,
                                                                        self._remote_object_status_buffer_subscription_manager)

        return remote_attributes

    def __setattr__(self, name: str, value: Any) -> None:
        if name.startswith(ATTRIBUTE_NAME_PREFIX) or name in dir(self.__class__):
            super().__setattr__(name, value)
        else:
            raise CannotSetRemoteAttributeError(f"Cannot set attribute '{name}' via RemoteObjectReference")

    def __getattribute__(self, item: str) -> Any:
        try:
            return super().__getattribute__(item)
        except AttributeError as e:
            # Handle ScopedAttributes
            if item.startswith(ATTRIBUTE_NAME_PREFIX):
                raise e
            try:
                # Attempt to handle automatically generated remote methods
                remote_method = self._remote_methods.get(item, None)
                if remote_method:
                    return remote_method

                # Attempt to handle automatically generated remote attributes
                remote_attr: Optional[RemoteObjectAttribute] = self._remote_attributes.get(item, None)
                if remote_attr:
                    return remote_attr.get_value()

                # Ensure that any remote_methods or remote_attributes exist - if not, it's likely that this AutoRemoteObjectReference has been incorrectly shared across Threads
                if len(self._remote_methods) == 0 and len(self._remote_attributes) == 0:
                    logger.warning(f"No remote methods or attributes found for AutoRemoteObjectReference for {self._interface} when attempting to retrieve '{item}' "
                                   f"- has it been incorrectly shared across threads?")

                # Handle methods that have specific implementations (ie, methods on the interface that are not abstract)
                return types.MethodType(getattr(self._interface, item), self)
            except AttributeError:
                raise e


class CannotSetRemoteAttributeError(RuntimeError):
    pass
