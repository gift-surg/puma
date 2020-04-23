from puma.attribute import child_only, child_scope_value, copied, factory, parent_only, unmanaged
from puma.primitives import ThreadLock
from puma.runnable import CommandDrivenRunnable


class ScopedAttributesTestRunnable(CommandDrivenRunnable):

    def __init__(self) -> None:
        super().__init__(self.__class__.__name__, [])


class PropertiesDefinedInInit:

    def __init__(self, parent_only_value: str, child_only_value: str, copied_value: str) -> None:
        self.init_parent_only = parent_only_value
        self.init_child_only = child_only_value
        self.init_copied = copied_value


class PropertiesDefinedAtClassLevel:
    parent_only_prop: str = parent_only("parent_only_prop")
    child_only_prop: str = child_only("child_only_prop")
    copied_prop: str = copied("copied_prop")
    child_only_not_set_in_parent: str = child_only("child_only_not_set_in_parent")
    unmanaged_prop: str = unmanaged("unmanaged_prop")

    def __init__(self, parent_only_value: str, child_only_value: str, copied_value: str) -> None:
        self.parent_only_prop = parent_only_value
        self.child_only_prop = child_scope_value(child_only_value)
        self.copied_prop = copied_value
        self.unmanaged_prop = "unmanaged-value"


class UnpickleablePropertiesDefinedInInit(PropertiesDefinedInInit):

    def __init__(self, parent_only_value: str, child_only_value: str, copied_value: str) -> None:
        super().__init__(parent_only_value, child_only_value, copied_value)
        self.init_unpickleable = ThreadLock()


class UnpickleablePropertiesDefinedAtClassLevel(PropertiesDefinedAtClassLevel):
    unpickleable_prop: ThreadLock = copied("unpickleable")

    def __init__(self, parent_only_value: str, child_only_value: str, copied_value: str) -> None:
        super().__init__(parent_only_value, child_only_value, copied_value)
        self.unpickleable_prop = factory(ThreadLock)


class PropertiesDefinedInInitAndAtClassLevel(PropertiesDefinedInInit, PropertiesDefinedAtClassLevel):

    def __init__(self, parent_only_value: str, child_only_value: str, copied_value: str) -> None:
        PropertiesDefinedInInit.__init__(self, parent_only_value, child_only_value, copied_value)
        PropertiesDefinedAtClassLevel.__init__(self, parent_only_value, child_only_value, copied_value)


class PropertiesDefinedInInitAndUnpickleablePropertiesDefinedAtClassLevel(PropertiesDefinedInInitAndAtClassLevel, UnpickleablePropertiesDefinedAtClassLevel):

    def __init__(self, parent_only_value: str, child_only_value: str, copied_value: str) -> None:
        PropertiesDefinedInInitAndAtClassLevel.__init__(self, parent_only_value, child_only_value, copied_value)
        UnpickleablePropertiesDefinedAtClassLevel.__init__(self, parent_only_value, child_only_value, copied_value)


class PropertiesDefinedAtClassLevelAndUnpickleablePropertiesDefinedInInit(PropertiesDefinedInInitAndAtClassLevel, UnpickleablePropertiesDefinedInInit):

    def __init__(self, parent_only_value: str, child_only_value: str, copied_value: str) -> None:
        PropertiesDefinedInInitAndAtClassLevel.__init__(self, parent_only_value, child_only_value, copied_value)
        UnpickleablePropertiesDefinedInInit.__init__(self, parent_only_value, child_only_value, copied_value)
