from typing import Callable
from unittest import skip

from puma.helpers.os import is_windows


def skip_on_windows_until_github_issue_11_is_resolved(func: Callable) -> Callable:
    if is_windows():
        return skip(reason=f'Skipping {func} until GitHub issue #11 is resolved')(func)
    else:
        return func
