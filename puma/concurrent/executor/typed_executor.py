from __future__ import annotations

from abc import ABC
from concurrent.futures import Executor
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures.thread import ThreadPoolExecutor
from itertools import zip_longest
from multiprocessing.context import BaseContext
from typing import Any, Callable, Iterable, List, Optional, Tuple, TypeVar, overload

from puma.concurrent import Timeout
from puma.concurrent.futures.typed_future import TypedFuture
from puma.context import ContextManager, Exit_1, Exit_2, Exit_3

# "submit" method type parameters
# SP# = Submit Parameter Type, SR = Submit Return Type
SP1 = TypeVar("SP1")
SP2 = TypeVar("SP2")
SP3 = TypeVar("SP3")
SP4 = TypeVar("SP4")
SP5 = TypeVar("SP5")
SP6 = TypeVar("SP6")
SP7 = TypeVar("SP7")
SP8 = TypeVar("SP8")
SP9 = TypeVar("SP9")
SP10 = TypeVar("SP10")
SR = TypeVar("SR")

# "map" method type parameters
# MP# = Map Parameter Type, MR = Map Return Type
MP1 = TypeVar("MP1")
MP2 = TypeVar("MP2")
MP3 = TypeVar("MP3")
MP4 = TypeVar("MP4")
MP5 = TypeVar("MP5")
MP6 = TypeVar("MP6")
MP7 = TypeVar("MP7")
MP8 = TypeVar("MP8")
MP9 = TypeVar("MP9")
MP10 = TypeVar("MP10")
MR = TypeVar("MR")


# TODO: Flake8 errors in this file are apparently fixed on master, but no new version has been released. https://github.com/PyCQA/pyflakes/issues/320
#  Wait for a while for this release before using comments to prevent errors

class TypedExecutor(ContextManager["TypedExecutor"], ABC):
    """A typed version of Python's build in concurrent.futures.Executor"""

    def __init__(self, executor: Executor) -> None:
        self._executor = executor

    def __enter__(self) -> "TypedExecutor":
        self._executor.__enter__()
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        self._executor.__exit__(exc_type, exc_value, traceback)

    # region map_tuple overloads
    @overload
    def map_tuple(self, fn: Callable[[], MR],
                  param_sets: List, timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    @overload  # noqa: F811
    def map_tuple(self, fn: Callable[[MP1], MR],
                  param_sets: Iterable[MP1], timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    @overload  # noqa: F811
    def map_tuple(self, fn: Callable[[MP1, MP2], MR],
                  param_sets: Iterable[Tuple[MP1, MP2]], timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    @overload  # noqa: F811
    def map_tuple(self, fn: Callable[[MP1, MP2, MP3], MR],
                  param_sets: Iterable[Tuple[MP1, MP2, MP3]], timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    @overload  # noqa: F811
    def map_tuple(self, fn: Callable[[MP1, MP2, MP3, MP4], MR],
                  param_sets: Iterable[Tuple[MP1, MP2, MP3, MP4]], timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    @overload  # noqa: F811
    def map_tuple(self, fn: Callable[[MP1, MP2, MP3, MP4, MP5], MR],
                  param_sets: Iterable[Tuple[MP1, MP2, MP3, MP4, MP5]], timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    @overload  # noqa: F811
    def map_tuple(self, fn: Callable[[MP1, MP2, MP3, MP4, MP5, MP6], MR],
                  param_sets: Iterable[Tuple[MP1, MP2, MP3, MP4, MP5, MP6]], timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    @overload  # noqa: F811
    def map_tuple(self, fn: Callable[[MP1, MP2, MP3, MP4, MP5, MP6, MP7], MR],
                  param_sets: Iterable[Tuple[MP1, MP2, MP3, MP4, MP5, MP6, MP7]], timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    @overload  # noqa: F811
    def map_tuple(self, fn: Callable[[MP1, MP2, MP3, MP4, MP5, MP6, MP7, MP8], MR],
                  param_sets: Iterable[Tuple[MP1, MP2, MP3, MP4, MP5, MP6, MP7, MP8]], timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    @overload  # noqa: F811
    def map_tuple(self, fn: Callable[[MP1, MP2, MP3, MP4, MP5, MP6, MP7, MP8, MP9], MR],
                  param_sets: Iterable[Tuple[MP1, MP2, MP3, MP4, MP5, MP6, MP7, MP8, MP9]], timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    @overload  # noqa: F811
    def map_tuple(self, fn: Callable[[MP1, MP2, MP3, MP4, MP5, MP6, MP7, MP8, MP9, MP10], MR],
                  param_sets: Iterable[Tuple[MP1, MP2, MP3, MP4, MP5, MP6, MP7, MP8, MP9, MP10]], timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    # endregion
    def map_tuple(self, fn: Callable[..., MR], param_sets: Iterable[Any], timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:  # noqa: F811
        try:
            restructured_params: Any = zip_longest(*param_sets)
        except TypeError as e:
            # Handle the case where a list of single values is given (when the method accepts only one argument)
            # vs. where a list of Tuples is given (for methods that accept several arguments)
            if "zip_longest argument #1 must support iteration" == str(e):
                restructured_params = (param_sets,)
            else:
                raise e
        return self._executor.map(fn, *restructured_params, timeout=timeout, chunksize=chunksize)

    # region map overloads
    @overload
    def map(self, fn: Callable[[MP1], MR],
            __param_1s: Iterable[MP1], timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    @overload  # noqa: F811
    def map(self, fn: Callable[[MP1, MP2], MR],
            __param_1s: Iterable[MP1], __param_2s: Iterable[MP2], timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    @overload  # noqa: F811
    def map(self, fn: Callable[[MP1, MP2, MP3], MR],
            __param_1s: Iterable[MP1], __param_2s: Iterable[MP2], __param_3s: Iterable[MP3], timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    @overload  # noqa: F811
    def map(self, fn: Callable[[MP1, MP2, MP3, MP4], MR],
            __param_1s: Iterable[MP1], __param_2s: Iterable[MP2], __param_3s: Iterable[MP3], __param_4s: Iterable[MP4],
            timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    @overload  # noqa: F811
    def map(self, fn: Callable[[MP1, MP2, MP3, MP4, MP5], MR],
            __param_1s: Iterable[MP1], __param_2s: Iterable[MP2], __param_3s: Iterable[MP3], __param_4s: Iterable[MP4], __param_5s: Iterable[MP5],
            timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    @overload  # noqa: F811
    def map(self, fn: Callable[[MP1, MP2, MP3, MP4, MP5, MP6], MR],
            __param_1s: Iterable[MP1], __param_2s: Iterable[MP2], __param_3s: Iterable[MP3], __param_4s: Iterable[MP4], __param_5s: Iterable[MP5],
            __param_6s: Iterable[MP6], timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    @overload  # noqa: F811
    def map(self, fn: Callable[[MP1, MP2, MP3, MP4, MP5, MP6, MP7], MR],
            __param_1s: Iterable[MP1], __param_2s: Iterable[MP2], __param_3s: Iterable[MP3], __param_4s: Iterable[MP4], __param_5s: Iterable[MP5],
            __param_6s: Iterable[MP6],
            __param_7s: Iterable[MP7], timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    @overload  # noqa: F811
    def map(self, fn: Callable[[MP1, MP2, MP3, MP4, MP5, MP6, MP7, MP8], MR],
            __param_1s: Iterable[MP1], __param_2s: Iterable[MP2], __param_3s: Iterable[MP3], __param_4s: Iterable[MP4], __param_5s: Iterable[MP5],
            __param_6s: Iterable[MP6], __param_7s: Iterable[MP7], __param_8s: Iterable[MP8], timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    @overload  # noqa: F811
    def map(self, fn: Callable[[MP1, MP2, MP3, MP4, MP5, MP6, MP7, MP8, MP9], MR],
            __param_1s: Iterable[MP1], __param_2s: Iterable[MP2], __param_3s: Iterable[MP3], __param_4s: Iterable[MP4], __param_5s: Iterable[MP5], __param_6s: Iterable[MP6],
            __param_7s: Iterable[MP7], __param_8s: Iterable[MP8], __param_9s: Iterable[MP9], timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    @overload  # noqa: F811
    def map(self, fn: Callable[[MP1, MP2, MP3, MP4, MP5, MP6, MP7, MP8, MP9, MP10], MR],
            __param_1s: Iterable[MP1], __param_2s: Iterable[MP2], __param_3s: Iterable[MP3], __param_4s: Iterable[MP4], __param_5s: Iterable[MP5],
            __param_6s: Iterable[MP6], __param_7s: Iterable[MP7], __param_8s: Iterable[MP8], __param_9s: Iterable[MP9], __param_10s: Iterable[MP10],
            timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:
        ...

    # endregion
    def map(self, fn: Callable[..., MR], *params: Any, timeout: Timeout = None, chunksize: int = 1) -> Iterable[MR]:  # noqa: F811
        return self._executor.map(fn, *params, timeout=timeout, chunksize=chunksize)

    # region submit overloads
    @overload
    def submit(self, fn: Callable[[], SR]) -> TypedFuture[SR]:
        ...

    @overload  # noqa: F811
    def submit(self, fn: Callable[[SP1], SR], param_1: SP1) -> TypedFuture[SR]:
        ...

    @overload  # noqa: F811
    def submit(self, fn: Callable[[SP1, SP2], SR], param_1: SP1, param_2: SP2) -> TypedFuture[SR]:
        ...

    @overload  # noqa: F811
    def submit(self, fn: Callable[[SP1, SP2, SP3], SR], param_1: SP1, param_2: SP2, param_3: SP3) -> TypedFuture[SR]:
        ...

    @overload  # noqa: F811
    def submit(self, fn: Callable[[SP1, SP2, SP3, SP4], SR], param_1: SP1, param_2: SP2, param_3: SP3, param_4: SP4) -> TypedFuture[SR]:
        ...

    @overload  # noqa: F811
    def submit(self, fn: Callable[[SP1, SP2, SP3, SP4, SP5], SR], param_1: SP1, param_2: SP2, param_3: SP3, param_4: SP4, param_5: SP5) -> TypedFuture[SR]:
        ...

    @overload  # noqa: F811
    def submit(self, fn: Callable[[SP1, SP2, SP3, SP4, SP5, SP6], SR],
               param_1: SP1, param_2: SP2, param_3: SP3, param_4: SP4, param_5: SP5, param_6: SP6) -> TypedFuture[SR]:
        ...

    @overload  # noqa: F811
    def submit(self, fn: Callable[[SP1, SP2, SP3, SP4, SP5, SP6, SP7], SR],
               param_1: SP1, param_2: SP2, param_3: SP3, param_4: SP4, param_5: SP5, param_6: SP6, param_7: SP7) -> TypedFuture[SR]:
        ...

    @overload  # noqa: F811
    def submit(self, fn: Callable[[SP1, SP2, SP3, SP4, SP5, SP6, SP7, SP8], SR],
               param_1: SP1, param_2: SP2, param_3: SP3, param_4: SP4, param_5: SP5, param_6: SP6, param_7: SP7, param_8: SP8) -> TypedFuture[SR]:
        ...

    @overload  # noqa: F811
    def submit(self, fn: Callable[[SP1, SP2, SP3, SP4, SP5, SP6, SP7, SP8, SP9], SR],
               param_1: SP1, param_2: SP2, param_3: SP3, param_4: SP4, param_5: SP5, param_6: SP6, param_7: SP7, param_8: SP8, param_9: SP9) -> TypedFuture[SR]:
        ...

    @overload  # noqa: F811
    def submit(self, fn: Callable[[SP1, SP2, SP3, SP4, SP5, SP6, SP7, SP8, SP9, SP10], SR],
               param_1: SP1, param_2: SP2, param_3: SP3, param_4: SP4, param_5: SP5, param_6: SP6, param_7: SP7, param_8: SP8, param_9: SP9, param_10: SP10) -> TypedFuture[SR]:
        ...

    # endregion
    def submit(self, fn: Callable[..., Any], *args: Iterable[Any], **kwargs: Any) -> Any:  # noqa: F811
        return self._executor.submit(fn, *args, **kwargs)

    def shutdown(self, wait: bool = True) -> None:
        return self._executor.shutdown(wait)


class TypedThreadPoolExecutor(TypedExecutor):

    def __init__(self, max_workers: Optional[int] = None, thread_name_prefix: str = "", initializer: Optional[Callable[..., None]] = None, initargs: Tuple = ()) -> None:
        super().__init__(ThreadPoolExecutor(max_workers, thread_name_prefix, initializer, initargs))


class TypedProcessPoolExecutor(TypedExecutor):

    def __init__(self, max_workers: Optional[int] = None, mp_context: Optional[BaseContext] = None, initializer: Optional[Callable[..., None]] = None,
                 initargs: Tuple = ()) -> None:
        # TODO: Ignore this type until we update to mypy 0.720. This currently causes lots of errors to be raised
        super().__init__(ProcessPoolExecutor(max_workers, mp_context, initializer, initargs))  # type: ignore
