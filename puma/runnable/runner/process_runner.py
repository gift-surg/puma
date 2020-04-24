import logging
import pickle
from multiprocessing import Process
from typing import Any, Optional, Set, Type, TypeVar

from puma.attribute import AccessibleScope, ProcessAction
from puma.attribute.attribute.scoped_attribute import ScopedAttribute
from puma.attribute.attribute.sharing_attribute_between_scopes_not_allowed_error import SharingAttributeBetweenProcessesNotAllowedError
from puma.attribute.mixin import ScopedAttributeState, ScopedAttributesBaseMixin
from puma.buffer import Buffer, MultiProcessBuffer
from puma.buffer.implementation.managed_queues import ManagedProcessQueue
from puma.context import Exit_1, Exit_2, Exit_3, ensure_used_within_context_manager
from puma.helpers.os import is_windows
from puma.logging import Logging, ManagedProcessLogQueue, ProcessLoggingMechanism
from puma.primitives import ProcessLock
from puma.runnable import Runnable
from puma.runnable.runner import Runner

BufferType = TypeVar("BufferType")

logger = logging.getLogger(__name__)


class ProcessRunner(Runner, Process):
    """Executes a Runnable within a separate process."""

    _instances_lock = ProcessLock()
    _instances = 0
    _process_log_queue: Optional[ManagedProcessQueue] = None
    _process_logging_mechanism: Optional[ProcessLoggingMechanism] = None

    def __init__(self, runnable: Runnable, name: Optional[str] = None) -> None:
        super().__init__(runnable, name)

    def __enter__(self) -> 'ProcessRunner':
        with ProcessRunner._instances_lock:
            ProcessRunner._instances += 1
            if ProcessRunner._instances == 1:
                logger.debug("Creating first instance of ProcessRunner, creating multiprocess logging mechanism")
                ProcessRunner._process_log_queue = self._log_queue_factory()
                ProcessRunner._process_log_queue.__enter__()
                ProcessRunner._process_logging_mechanism = ProcessLoggingMechanism(ProcessRunner._process_log_queue)
                ProcessRunner._process_logging_mechanism.__enter__()
        assert ProcessRunner._process_log_queue
        self._child_process_logging_config = Logging.get_child_process_logging_config(ProcessRunner._process_log_queue)
        super().__enter__()
        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        try:
            super().__exit__(exc_type, exc_value, traceback)
        finally:
            with ProcessRunner._instances_lock:
                ProcessRunner._instances -= 1
                if ProcessRunner._instances == 0:
                    logger.debug("Destroying last instance of ProcessRunner, removing multiprocess logging mechanism")
                    if ProcessRunner._process_logging_mechanism:
                        ProcessRunner._process_logging_mechanism.__exit__(exc_type, exc_value, traceback)
                        ProcessRunner._process_logging_mechanism = None
                    if ProcessRunner._process_log_queue:
                        ProcessRunner._process_log_queue.__exit__(exc_type, exc_value, traceback)

    def __getstate__(self) -> ScopedAttributeState:
        ret = super().__getstate__()
        # Serialise the instance-counting mechanism and the logging queue to the new process, because these should be system-global
        ret.attributes['$cls$_instances_lock'] = ProcessRunner._instances_lock
        with ProcessRunner._instances_lock:
            ret.attributes['$cls$_instances'] = ProcessRunner._instances
            ret.attributes['$cls$_process_log_queue'] = ProcessRunner._process_log_queue
        return ret

    def __setstate__(self, state: ScopedAttributeState) -> None:
        ProcessRunner._instances_lock = state.attributes.pop('$cls$_instances_lock')
        with ProcessRunner._instances_lock:
            ProcessRunner._instances = state.attributes.pop('$cls$_instances')
            ProcessRunner._process_log_queue = state.attributes.pop('$cls$_process_log_queue')
            ProcessRunner._process_logging_mechanism = None  # Only used by the main process, not children
        super().__setstate__(state)

    def get_name(self) -> str:
        """Overload, delegating to the process"""
        return self.name

    def set_name(self, name: str) -> None:
        """Overload, delegating to the process"""
        if not name:
            raise ValueError("A name must be supplied")
        self.name = name

    @ensure_used_within_context_manager
    def start(self) -> None:
        self._runnable.runner_accessor.assert_comms_not_already_set()
        self._handle_scoped_attributes_in_parent_scope(self._runnable, set())
        """Overload, delegating to the process"""
        self._runnable.runner_accessor.set_command_buffer(self._command_buffer)
        self._runnable.runner_accessor.set_status_buffer_subscription(self._status_buffer_subscription)

        try:
            Process.start(self)  # calls run() in a new process
        except TypeError as e:
            self._handle_type_error(e)

    def run(self) -> None:
        Logging.init_child_process_logging(self._child_process_logging_config)
        super().run()

    def _perform_join(self, timeout: Optional[float] = None) -> None:
        """Overload, delegating to the process"""
        Process.join(self, timeout)

    def is_alive(self) -> bool:
        """Overload, delegating to the process"""
        return Process.is_alive(self)

    def _handle_type_error(self, e: TypeError) -> None:
        if str(e).startswith("can't pickle "):
            runnable_state = self._runnable.__getstate__()
            for key, item in runnable_state.attributes.items():
                try:
                    pickle.dumps(item)
                except BaseException as pe:
                    # Ignore ManagedProcessQueue errors
                    if "Queue objects should only be shared between processes through inheritance" != str(pe):
                        logger.error(f"Unable to pickle: {key} from {self._runnable}", exc_info=True)
        raise e

    def _buffer_factory(self, element_type: Type[BufferType], size: int, name: str, warn_on_discard: Optional[bool] = True) -> Buffer[BufferType]:
        return MultiProcessBuffer(size, name, warn_on_discard)

    def _log_queue_factory(self) -> ManagedProcessLogQueue:
        return ManagedProcessLogQueue(name='logging queue')

    def _handle_scoped_attributes_in_parent_scope(self, obj: Any, object_recursion_tracker: Set[Any]) -> None:

        def attribute_callback(name: str, attribute: ScopedAttribute) -> None:
            # Handle any ScopedAttributes
            if attribute.accessible_scope == AccessibleScope.shared:
                if attribute.process_action == ProcessAction.NOT_ALLOWED:
                    raise SharingAttributeBetweenProcessesNotAllowedError(name)
                attribute_value = getattr(obj, name)
                # Recursively handle ScopedAttributes whilst avoiding infinite loops
                if isinstance(attribute_value, ScopedAttributesBaseMixin) and attribute_value not in object_recursion_tracker:
                    object_recursion_tracker.add(attribute_value)
                    self._handle_scoped_attributes_in_parent_scope(attribute_value, object_recursion_tracker)

        self._iterate_over_all_scoped_attributes(obj, attribute_callback)

    def _handle_scoped_attributes_in_child_scope(self, obj: Any, object_recursion_tracker: Set[Any]) -> None:
        if is_windows():
            # No need to handle child attributes on Windows, as they will have already been managed in __getstate__ and __setstate__
            return
        super()._handle_scoped_attributes_in_child_scope(obj, object_recursion_tracker)

    def _handle_individual_scoped_attribute_in_child_scope(self, obj: Any, name: str, attribute: ScopedAttribute) -> None:
        if attribute.process_action == ProcessAction.SET_TO_NONE:
            setattr(obj, name, None)
