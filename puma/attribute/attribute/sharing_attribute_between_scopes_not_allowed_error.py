from puma.attribute import ThreadAction


class SharingAttributeBetweenScopesNotAllowedError(TypeError):

    def __init__(self, attribute_name: str, scope_type: str, action_type: str) -> None:
        super().__init__(f"Attribute '{attribute_name}' may not be passed between {scope_type} as its {action_type} is '{ThreadAction.NOT_ALLOWED.name}'")


class SharingAttributeBetweenThreadsNotAllowedError(SharingAttributeBetweenScopesNotAllowedError):

    def __init__(self, attribute_name: str) -> None:
        super().__init__(attribute_name, "Threads", "ThreadAction")


class SharingAttributeBetweenProcessesNotAllowedError(SharingAttributeBetweenScopesNotAllowedError):

    def __init__(self, attribute_name: str) -> None:
        super().__init__(attribute_name, "Processes", "ProcessAction")
