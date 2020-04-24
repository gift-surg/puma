from typing import TypeVar, cast

from puma.attribute.attribute_proxy.attribute_proxy import AttributeProxy
from puma.attribute.attribute_proxy.attribute_proxy_factory import AttributeProxyFactory, SharedWithinInstanceAttributeProxyFactory

T = TypeVar("T")


def proxy() -> "AttributeProxy[T]":
    return cast(AttributeProxy[T], AttributeProxyFactory())


def shared_within_instance_proxy() -> AttributeProxy[T]:
    return cast(AttributeProxy[T], SharedWithinInstanceAttributeProxyFactory())
