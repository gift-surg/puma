import os
import threading


def get_current_scope_id() -> str:
    return f"{os.getpid()}-{threading.get_ident()}"
