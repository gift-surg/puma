class MustBeContextManagedError(RuntimeError):

    def __init__(self) -> None:
        super().__init__("Must be context managed")
