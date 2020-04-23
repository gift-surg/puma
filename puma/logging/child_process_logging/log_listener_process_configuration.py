from abc import ABC, abstractmethod
from typing import Dict, List

from puma.buffer.implementation.managed_queues import ManagedProcessQueue
from puma.logging import LogLevel


class _LoggingConfigAction(ABC):
    """Base class for records of logging actions being recorded for later replay."""

    @abstractmethod
    def do_init(self) -> None:
        raise NotImplementedError()


class _InitLoggingConfigAction(_LoggingConfigAction):
    """Records a call to LoggingUtils.init_logging, for later replay."""

    def __init__(self, config_filename: str):
        self._config_filename = config_filename

    def do_init(self) -> None:
        puma.logging.logging.Logging.init_logging(self._config_filename)


class _OverrideGlobalLevelConfigAction(_LoggingConfigAction):
    """Records a call to LoggingUtils.override_global_level, for later replay."""

    def __init__(self, global_level: LogLevel):
        self._global_level = global_level

    def do_init(self) -> None:
        puma.logging.logging.Logging.override_global_level(self._global_level)


class _OverrideSectionsConfigAction(_LoggingConfigAction):
    """Records a call to LoggingUtils.override_sections, for later replay."""

    def __init__(self, section_overrides: Dict[str, LogLevel]):
        self._section_overrides = section_overrides

    def do_init(self) -> None:
        puma.logging.logging.Logging.override_sections(self._section_overrides)


class _OverrideRootConfigAction(_LoggingConfigAction):
    """Records a call to LoggingUtils.override_root, for later replay."""

    def __init__(self, root_override: LogLevel):
        self._root_override = root_override

    def do_init(self) -> None:
        puma.logging.logging.Logging.override_root(self._root_override)


class _MemoryLogHandlerConfig:
    """Records a call to LoggingUtils.add_memory_log_handler, for later replay."""

    def __init__(self, capture_queue: ManagedProcessQueue, level: LogLevel):
        self._capture_queue = capture_queue
        self._level = level
        self._paused = False

    def do_add_handler(self) -> None:
        puma.logging.logging.Logging.add_memory_log_handler(self._capture_queue, self._level)

    def pause(self) -> None:
        self._paused = True

    def resume(self) -> None:
        self._paused = False

    def is_paused(self) -> bool:
        return self._paused


class LogListenerProcessConfiguration:
    """Records the logging initialisation that has been applied in the main process, so it can be re-applied in the log listening process.

    Separated from the implementation because of circular dependency problems.
    Concrete instances of this interface are obtained from the LogListenerProcessConfigurationFactory.

    It records logging settings, and also any in-memory log handlers that are currently active.
    """

    def __init__(self) -> None:
        self._init_actions: List[_LoggingConfigAction] = []
        self._memory_handlers: Dict[str, _MemoryLogHandlerConfig] = {}

    def record_init_logging_action(self, config_filename: str) -> None:
        self._init_actions.append(_InitLoggingConfigAction(config_filename))

    def record_override_global_level_action(self, global_level: LogLevel) -> None:
        self._init_actions.append(_OverrideGlobalLevelConfigAction(global_level))

    def record_override_sections_action(self, section_overrides: Dict[str, LogLevel]) -> None:
        self._init_actions.append(_OverrideSectionsConfigAction(section_overrides))

    def record_override_root_action(self, root_override: LogLevel) -> None:
        self._init_actions.append(_OverrideRootConfigAction(root_override))

    def record_add_memory_log_handler_action(self, capture_queue: ManagedProcessQueue, level: LogLevel, key_name: str) -> None:
        self._memory_handlers[key_name] = _MemoryLogHandlerConfig(capture_queue, level)

    def record_remove_memory_log_handler_action(self, key_name: str) -> None:
        self._memory_handlers.pop(key_name, None)

    def record_pause_memory_log_handler_action(self, key_name: str) -> None:
        handler = self._memory_handlers.get(key_name, None)
        if handler:
            handler.pause()

    def record_resume_memory_log_handler_action(self, key_name: str) -> None:
        handler = self._memory_handlers.get(key_name, None)
        if handler:
            handler.resume()

    def clear(self) -> None:
        self._init_actions.clear()
        self._memory_handlers.clear()

    def playback_actions(self) -> None:
        for action in self._init_actions.copy():
            action.do_init()
        for memory_handler in list(self._memory_handlers.values()):
            if not memory_handler.is_paused():
                memory_handler.do_add_handler()
        self.clear()


import puma.logging.logging  # noqa: E402, I100, I202
# This import is at the end of the file, and has this 'import X' form, because
# of the circular dependency between LoggingUtils and LogListenerProcessConfiguration
