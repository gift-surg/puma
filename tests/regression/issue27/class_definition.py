from abc import ABC, abstractmethod

from puma.runnable.remote_execution import BaseRemoteObjectReference


class MyInterface(ABC):

    @abstractmethod
    def my_method(self, a: int, b: str) -> bool:
        raise NotImplementedError()


class ProxyMyInterface(BaseRemoteObjectReference[MyInterface], MyInterface, ABC):

    def my_method(self, a: int, b: str) -> bool:
        return self._remote_method(self._wrapped_instance.my_method).call(a, b)
