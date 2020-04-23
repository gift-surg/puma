import importlib
import itertools
import logging
import logging.config
import logging.handlers
import os
import sys
from logging import LogRecord
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Union, cast, no_type_check

import yaml

from puma.buffer.implementation.managed_queues import ManagedProcessQueue
from puma.helpers.class_name import get_class_fully_qualified_name, get_fully_qualified_name
from puma.helpers.string import safe_str
from puma.logging import LogLevel
from puma.logging.child_process_logging.child_process_configuration import ChildProcessConfiguration
from puma.logging.child_process_logging.current_logging_configuration import CurrentLoggingConfiguration, LogListenerProcessConfiguration
from puma.primitives import ThreadRLock

DEFAULT_LOG_FILE = 'logging_dev.yaml'

_CAPTURE_HANDLER_PREFIX = "$capture$"

"""
If you want to configure logging, the easiest way is to call Logging.init_logging() at the start of your program. This takes a yaml file; the default ‘logging_dev.yaml’ is
suitable for development.

In order to get cross-process logging to work, you don’t have to do anything to your code except make sure your processes are run using ProcessRunner. You do NOT need to
initialise logging. By default, you will get warning-level logging, to console, from all processes.

You can also override certain logging behaviours, which may be useful when investigating an issue. For example::
    Logging.init_logging()
    Logging.override_sections({‘puma.buffer’: LogLevel.debug})


If you want to capture log output, in order to check that certain errors or warning are logged, you can use CaptureLogs::
    with CaptureLogs(LogLevel.debug) as log_context:
        self._logger.debug("Instance")
        records = log_context.pop_captured_records()
        self.assertEqual(['Instance'], records.get_lines(timestamp=False, level=False, line_separators=False))

You can filter the captured lines in various ways, e.g.::
    records.with_levels_in({LogLevel.debug, LogLevel.error}).containing_message("my error").get_lines())

There is also a decorator you can use if you want to check that some code does NOT log any errors or warnings::
    @assert_no_warnings_or_errors_logged
    def test(self) -> None:
        logging.debug("No problem")


If you mess about with logging in a test case, you should reset the logging system in the tearDown(), otherwise the changes you’ve made will persist to all following tests.
This is done with Logging.reset_logging(). Note that this will break all existing loggers, so in such a case you would need to create the logger in the setup() code rather
than at module level. For example::
    def setUp(self) -> None:
        Logging.override_global_level(LogLevel.debug)
        self._logger = logging.getLogger(__name__)

    def tearDown(self) -> None:
        Logging.reset_logging(

    def test(self) -> None:
        self._logger.debug(“stuff”)
"""


def _robust_is_instance(o: object, t: type) -> bool:
    # Use instead of isinstance() for types loaded from the logging module, which may have been reloaded, causing isinstance() to always return false
    # if checking the type of an object created before the reload
    if isinstance(o, t):
        return True

    if get_fully_qualified_name(o) == get_class_fully_qualified_name(t):
        return True

    for b in o.__class__.__bases__:
        if get_class_fully_qualified_name(t) == get_class_fully_qualified_name(b):
            return True
    return False


class Logging:
    # There will be one instance of each of these objects *per process*
    __lock = ThreadRLock()
    __history: CurrentLoggingConfiguration = CurrentLoggingConfiguration()
    __logger_dict: Optional[Dict[str, Any]] = None  # cached reference to logging dictionary
    __pre_reset_loggers: Set[logging.Logger] = set()  # loggers that existed before we reset the logging system, so we can modify them when settings change

    @classmethod
    def init_logging(cls, config_filename: str = DEFAULT_LOG_FILE) -> None:
        """Initialise logging from a YAML-format configuration file. If any loggers log to file, the specified directories will be created if necessary.

        Call one or more of the "override" methods afterwards to modify the logging, e.g. to set or change the log level for a particular module or package.
        This does not affect any existing child processes, which continue to use the logging configured when they were started.

        :param config_filename: Path of YAML-format configuration file. If the file can't be found, tries looking for it in the same folder as this source file.
        """
        with cls.__lock:
            cls.init_logging_impl(config_filename)
            cls._make_existing_loggers_act_as_currently_configured(cls.__pre_reset_loggers)
            cls.__history.record_init_logging_action(config_filename)

    @classmethod
    def init_logging_from_dict(cls, config: Dict[str, Any]) -> None:
        """Initialise logging from a dictionary, as would be read from a configuration file. If any loggers log to file, the specified directories will be created if necessary.

        The dictionary should contain only simple values (strings, integers etc.) as would be read from a file - do not use this mechanism to set up logging
        to queues, for example.

        Call one or more of the "override" methods afterwards to modify the logging, e.g. to set or change the log level for a particular module or package.
        This does not affect any existing child processes, which continue to use the logging configured when they were started.

        :param config: Dictionary to initialise from
        """
        with cls.__lock:
            cls.init_logging_from_dict_impl(config)
            cls._make_existing_loggers_act_as_currently_configured(cls.__pre_reset_loggers)
            cls.__history.record_init_logging_from_dict_action(config)

    @classmethod
    def override_global_level(cls, global_level: LogLevel) -> None:
        """Override the logging level in all configured section, to the given level.

        This does not affect any existing child processes, which continue to use the logging configured when they were started.
        """
        with cls.__lock:
            cls.override_global_level_impl(global_level)
            cls._make_existing_loggers_act_as_currently_configured(cls.__pre_reset_loggers)
            cls.__history.record_override_global_level_action(global_level)

    @classmethod
    def override_sections(cls, section_overrides: Dict[str, LogLevel]) -> None:
        """Set/override the logging level for one or more section of code, for example Logging.override_sections({"puma": LogLevel.debug})

        Overriding a section affects all loggers in that package or any child packages, for example setting the level for package "a.b" also affects package
        "a.b.c", but not "a".

        If init_logging() has not been called to set up some other arrangement, then all loggers will propagate to the root logger, so setting the level of
        the root logger will set the level of all loggers.

        This does not affect any existing child processes, which continue to use the logging configured when they were started.
        This modifies or adds sections into the 'loggers' section of the configuration.
        """
        with cls.__lock:
            cls.override_sections_impl(section_overrides)
            cls._make_existing_loggers_act_as_currently_configured(cls.__pre_reset_loggers)
            cls.__history.record_override_sections_action(section_overrides)

    @classmethod
    def override_root(cls, root_override: LogLevel) -> None:
        """Override the root logging level, used by all logging if logging has not been initialised, and also used when no logger is matched.

        If init_logging() has not been called to set up some other arrangement, then all loggers will propagate to the root logger, so changing the
        root level will be the same as changing the global level. If init_logging() has been called then only root logging and any loggers that propagate to
        it will be affected.

        This does not affect any existing child processes, which continue to use the logging configured when they were started.
        """
        with cls.__lock:
            cls.override_root_impl(root_override)
            cls._make_existing_loggers_act_as_currently_configured(cls.__pre_reset_loggers)
            cls.__history.record_override_root_action(root_override)

    @classmethod
    def reset_logging(cls) -> None:
        """Resets logging to its default case, as if never initialised. The log level will be Warning throughout.

        Note that any module-level loggers will no longer function as expected after this. Therefore, loggers will need to be obtained again using getLogger.
        This means that you cannot use module-level loggers in test cases that reset the logging in their tearDown function.
        """
        with cls.__lock:
            cls.reset_logging_impl()  # adds existing loggers to cls.__pre_reset_loggers before resetting the system
            cls._make_existing_loggers_act_as_currently_configured(cls.__pre_reset_loggers)
            cls.__history.clear()

    @classmethod
    def init_child_process_logging(cls, logging_process_config: ChildProcessConfiguration) -> None:
        """Used by child processes, which log to a queue. Applies the configuration returned by get_child_process_logging_config()."""
        with cls.__lock:
            cls.init_child_process_logging_impl(logging_process_config)
            cls.__history = logging_process_config.parent_history

    @classmethod
    def init_log_listener_process_logging(cls, logging_process_config: LogListenerProcessConfiguration) -> None:
        """Used only by the process that receives log messages from a queue and re-logs them. Applies the configuration returned by get_log_listener_process_logging_config()."""
        with cls.__lock:
            cls.init_log_listener_process_logging_impl(logging_process_config)
            cls.__history.clear()

    @classmethod
    def add_memory_log_handler(cls, capture_queue: ManagedProcessQueue, level: LogLevel) -> str:
        # Used by CaptureLogs to add another capture
        with cls.__lock:
            name = cls.add_memory_log_handler_impl(capture_queue, level)
            cls.__history.record_add_memory_log_handler_action(capture_queue, level, name)
            return name

    @classmethod
    def remove_memory_log_handler(cls, name: str) -> None:
        # Used by CaptureLogs at the end of capture
        with cls.__lock:
            cls.remove_memory_log_handler_impl(name)
            cls.__history.record_remove_memory_log_handler_action(name)

    @classmethod
    def pause_memory_log_handler(cls, name: str) -> Optional[int]:
        # Used by CaptureLogs to pause a parent handler while it is being "shielded" by a child handler
        with cls.__lock:
            old_level = cls.pause_handler_impl(name)
            if not old_level:
                return None
            cls.__history.record_pause_memory_log_handler_action(name)
            return old_level

    @classmethod
    def resume_memory_log_handler(cls, name: str, old_level: int) -> None:
        # Used by CaptureLogs to resume a parent handler that was paused with pause_memory_log_handler()
        with cls.__lock:
            cls.resume_handler_impl(name, old_level)
            cls.__history.record_resume_memory_log_handler_action(name)

    @classmethod
    def get_child_process_logging_config(cls, logging_queue: ManagedProcessQueue[LogRecord]) -> ChildProcessConfiguration:
        """Used by ProcessRunner, returns logging configuration for child processes, based on the current configuration .

        Uses the configured loggers, redirecting them to a queue handler. In this way, filtering is performed at source, so we don't put anything
        onto the queue which would be discarded in the listener process.
        """
        with cls.__lock:
            return ChildProcessConfiguration(cls.get_child_process_logging_config_impl(logging_queue), cls.__history)

    @classmethod
    def get_current_logging_config(cls) -> CurrentLoggingConfiguration:
        """Gets the current logging configuration, for later replay by restore_current_logging_config()."""
        with cls.__lock:
            return cls.__history.copy()

    @classmethod
    def get_log_listener_process_logging_config(cls) -> LogListenerProcessConfiguration:
        """Used by ProcessRunner, gets the configuration to be used by the process that listens on the queue and finally writes to log file.

        This is passed across the processing boundary and used by init_log_listener_process_logging() in the listening process.

        This is the same as the parent's configuration, since it actually does the logging.

        I have tried and failed to pass the current process's configuration over to the child process; some of it just doesn't pass, such as console loggers.
        Therefore there are two options:
        1. Listen and re-log in a *thread* of the main process, rather than in a *process"; or
        2. Record the configuration actions that have been called on the main process, and play them back in the listening process.
        Because python's threading isn't very good, I have taken the second approach.
        """
        with cls.__lock:
            return cls.__history.copy()

    @classmethod
    def restore_current_logging_config(cls, config: CurrentLoggingConfiguration) -> None:
        """Restores logging to the state given by get_current_logging_config()."""
        with cls.__lock:
            cls._restore_current_logging_config_impl(config)
            cls._make_existing_loggers_act_as_currently_configured(cls.__pre_reset_loggers)
            cls.__history = config

    @classmethod
    def print_all_loggers_for_process(cls) -> None:
        # For investigating logging issues, prints (not logs!) a description of all the loggers currently configured in the process
        for logger in cls._get_current_loggers():
            cls._print_logger_info(logger)
        print()

    @classmethod
    def print_logging_mechanics_for_logger(cls, logger: logging.Logger) -> None:
        # For investigating logging issues, prints (not logs!) a description of the loggers and handlers that will be invoked when logging using the given logger
        cls._print_logger_info(logger)
        print()

    @classmethod
    def init_logging_impl(cls, config_filename: str) -> None:
        config_path = cls._find_config_file(config_filename)
        with config_path.open('rt') as f:
            config = yaml.safe_load(f.read())
        cls._apply_dictionary_config(config)

    @classmethod
    def init_logging_from_dict_impl(cls, config: Dict[str, Any]) -> None:
        cls._apply_dictionary_config(config)

    @classmethod
    def init_child_process_logging_impl(cls, logging_process_config: ChildProcessConfiguration) -> None:
        cls._close_existing_handlers()
        cls._apply_dictionary_config(logging_process_config.child_dict)

    @classmethod
    def init_log_listener_process_logging_impl(cls, logging_process_config: LogListenerProcessConfiguration) -> None:
        cls._restore_current_logging_config_impl(logging_process_config)

    @classmethod
    def _restore_current_logging_config_impl(cls, config: CurrentLoggingConfiguration) -> None:
        cls.reset_logging_impl()
        config.playback_actions()

    @classmethod
    def _make_existing_loggers_act_as_currently_configured(cls, existing_loggers: Iterable[logging.Logger]) -> None:
        for existing_logger in existing_loggers:
            if existing_logger.name == 'root' or _robust_is_instance(existing_logger, logging.RootLogger):
                name = ''
            else:
                name = existing_logger.name
            new_logger = logging.getLogger(name)
            if new_logger == existing_logger:
                continue

            # Loggers remember whether they are enabled at a specific level, in a cache. Unfortunately this will be out of date.
            # In normal use, Logger.setLevel clears all caches; but this doesn't work after we've reset the logging system, because these orphaned
            # loggers are not in the current dictionary and so are not cleared.
            existing_logger._cache.clear()  # type: ignore  # noinspection PyProtectedMemberInspection  # pylint: disable=protected-access

            existing_logger.setLevel(new_logger.level)
            existing_logger.parent = new_logger.parent
            existing_logger.propagate = new_logger.propagate
            existing_logger.handlers = new_logger.handlers.copy()
            existing_logger.disabled = new_logger.disabled
            existing_logger.filters = new_logger.filters.copy()

    @classmethod
    def override_global_level_impl(cls, global_level: LogLevel) -> None:
        cls.override_sections_impl({logger.name: global_level for logger in cls._get_current_loggers(including_root=True)})

    @staticmethod
    def logger_name_for_sort(name: str) -> str:
        if not name or name == 'root':
            return ''
        return name

    @classmethod
    def override_sections_impl(cls, section_overrides: Dict[str, LogLevel]) -> None:
        # working from the most specific logger to the deepest child (root), make sure all loggers have at least one handler, otherwise
        # they would fall back to the handler-of-last-resort; we simply give them a handler that behaves like the handler of last resort if necessary,
        # which is more convenient than dealing with that special case everywhere
        names = sorted(section_overrides.keys(), key=Logging.logger_name_for_sort, reverse=True)
        for name in names:
            logger = cls._get_current_logger_for(name)
            cls._make_sure_logger_has_handler(logger)

        # Do the rest in the opposite direction: start with root and work to more specific handlers
        names.reverse()

        # Asking the logging system for a logger of interest helps it to initialise cleanly, replacing placeholders
        for name in names:
            logging.getLogger(name)

        # Set the levels as instructed
        for name in names:
            level = section_overrides[name]
            logger = cls._get_current_logger_for(name)
            logger.setLevel(level.value)
            for child_logger in cls._get_current_child_loggers(name):
                child_logger.setLevel(level.value)
            for handler in cls._get_handlers_for(logger):
                handler.setLevel(level.value)

    @classmethod
    def override_root_impl(cls, root_override: LogLevel) -> None:
        cls.override_sections_impl({'root': root_override})

    @classmethod
    def _get_handlers_for(cls, logger: logging.Logger) -> List[logging.Handler]:
        ret = logger.handlers.copy()
        if logger.propagate and logger.parent and _robust_is_instance(logger.parent, logging.Logger):
            ret.extend(cls._get_handlers_for(cast(logging.Logger, logger.parent)))
        return ret

    @classmethod
    def _make_sure_logger_has_handler(cls, logger: logging.Logger) -> None:
        # We don't want to have to worry about the handler of last resort as a special case; so if a logger
        # has no handler, give it a default one
        if cls._get_handlers_for(logger):
            return

        deepest_logger = cls._get_deepest_parent(logger)
        default_handler = logging.StreamHandler(sys.stdout)
        default_handler.setLevel(logging.WARN)
        deepest_logger.addHandler(default_handler)

    @classmethod
    def reset_logging_impl(cls) -> None:
        cls.__pre_reset_loggers.update(cls._get_current_loggers())

        cls._close_existing_handlers()
        logging.shutdown()
        importlib.reload(logging)
        cls.__logger_dict = None  # force the cache to reload

    @classmethod
    def add_memory_log_handler_impl(cls, capture_queue: ManagedProcessQueue, level: LogLevel) -> str:
        name = cls._get_next_capture_handler_name()

        handler = logging.handlers.QueueHandler(capture_queue)
        handler.setLevel(level.value)
        handler.name = name

        for logger in cls._get_current_loggers():
            # find the parent-most logger that will receive events propagated from this logger
            ancestor = cls._get_deepest_parent(logger)
            # add the in-memory handler to this ancestor
            if handler not in ancestor.handlers:
                ancestor.addHandler(handler)

        return name

    @classmethod
    # noinspection PyTypeChecker
    @no_type_check
    def _get_deepest_parent(cls, logger: logging.Logger) -> logging.Logger:
        ancestor = logger
        while ancestor.propagate and ancestor.parent:
            ancestor = ancestor.parent
        return ancestor

    @classmethod
    def remove_memory_log_handler_impl(cls, name: str) -> None:
        for logger in cls._get_current_loggers():
            for handler in cls._get_logger_handlers(logger).copy():
                if handler.name == name:
                    logger.removeHandler(handler)

    @classmethod
    def pause_handler_impl(cls, name: str) -> Optional[int]:
        handler = cls._get_current_handler_named(name)
        if not handler:
            return None

        old_level = handler.level
        handler.setLevel(logging.CRITICAL)
        return old_level

    @classmethod
    def resume_handler_impl(cls, name: str, old_level: int) -> None:
        handler = cls._get_current_handler_named(name)
        if handler:
            handler.setLevel(old_level)

    @classmethod
    def get_child_process_logging_config_impl(cls, logging_queue: ManagedProcessQueue[LogRecord]) -> Dict[str, Any]:
        # Returns the configuration to be used by child processes. This is the same as the current configuration, except that
        # the log handlers are removed and replaced with a single QueueHandler. This ensures that logging is filtered at source,
        # so that log records are only put on the queue if they are going to get logged.
        loggers = cls._get_current_loggers(including_root=False)
        root_logger = logging.getLogger('')

        ret = {
            'version': 1,
            'disable_existing_loggers': False,
            'handlers': {
                'queue': {
                    'class': 'logging.handlers.QueueHandler',
                    'queue': logging_queue
                },
            },
            'loggers': {logger.name: {'level': logger.level, 'handlers': ['queue'], 'propagate': logger.propagate} for logger in loggers
                        if cls._logger_calls_own_handlers(logger)},
            'root': {
                'level': root_logger.level,
                'handlers': ['queue']
            }
        }
        cls._ensure_basic_logging_sections_present(ret)
        return ret

    @classmethod
    def _logger_calls_own_handlers(cls, logger: logging.Logger) -> bool:
        for handler in logger.handlers:
            if handler.level <= logger.level:
                return True
        return False

    @classmethod
    def _apply_dictionary_config(cls, config: Dict[str, Any]) -> None:
        cls._ensure_basic_logging_sections_present(config)
        cls._create_directories_for_configured_log_files(config)
        logging.config.dictConfig(config)

    @classmethod
    def _get_current_loggers(cls, *, including_root: bool = True) -> List[logging.Logger]:
        ret = []
        for logger in cls._get_logger_dict().values():
            if _robust_is_instance(logger, logging.Logger):
                ret.append(logger)
        if including_root:
            root_logger = logging.getLogger('')
            if _robust_is_instance(root_logger, logging.Logger):
                ret.append(root_logger)
        return ret

    @classmethod
    def _get_current_logger_for(cls, section: str) -> logging.Logger:
        if section == 'root':
            section = ''
        while section:
            logger = cls._get_logger_dict().get(section)
            if logger and not _robust_is_instance(logger, logging.PlaceHolder):
                return cast(logging.Logger, logger)
            index = section.rfind('.')
            section = section[:index] if index > 0 else ""
        return logging.getLogger('')

    @classmethod
    def _get_current_child_loggers(cls, section: str) -> List[logging.Logger]:
        ret = []
        for name, logger in cls._get_logger_dict().items():
            if name.startswith(section + ".") and not _robust_is_instance(logger, logging.PlaceHolder):
                ret.append(logger)
        return ret

    @classmethod
    def _get_logger_handlers(cls, logger: logging.Logger) -> List[logging.Handler]:
        if hasattr(logger, 'handlers'):
            return logger.handlers
        else:
            return []

    @classmethod
    def _get_current_handler_named(cls, name: str) -> Optional[logging.Handler]:
        for logger in cls._get_current_loggers():
            for handler in cls._get_logger_handlers(logger):
                if handler.name == name:
                    return handler
        return None

    @classmethod
    def _get_next_capture_handler_name(cls) -> str:
        for i in itertools.count():
            name = f"{_CAPTURE_HANDLER_PREFIX}{i}"
            if not cls._get_current_handler_named(name):
                return name
        return ''  # will never get here, but keeps mypy happy

    @staticmethod
    def _find_config_file(config_filename: str) -> Path:
        ret = Path(config_filename)
        if not ret.is_file():
            my_folder = os.path.dirname(os.path.realpath(__file__))
            ret = Path(my_folder).joinpath(config_filename)
        if not ret.is_file():
            raise ValueError(f"Could not find the logging config file '{config_filename}'")
        return ret

    @staticmethod
    def _ensure_basic_logging_sections_present(config: Dict[str, Any]) -> None:
        if 'version' not in config:
            config['version'] = 1
        if 'disable_existing_loggers' not in config:
            config['disable_existing_loggers'] = False
        if 'handlers' not in config:
            config['handlers'] = {}
        if 'loggers' not in config:
            config['loggers'] = {}
        if 'root' not in config:
            config['root'] = {
                'level': 'DEBUG',
                'handlers': []
            }

    @classmethod
    def _create_directories_for_configured_log_files(cls, config: Dict[str, Any]) -> None:
        # Using the logging configuration read from file, find any file loggers and ensure the relevant directories are created
        if 'handlers' not in config:
            return
        handlers = cast(Dict[str, Dict[str, str]], config['handlers'])
        for handler in handlers.values():
            if _robust_is_instance(handler, Dict):
                filename = handler.get('filename', None)  # logging.FileHandler and its derived handlers have this parameter
                if filename:
                    cls._create_log_directory(filename)

    @classmethod
    def _get_all_current_handlers(cls) -> List[logging.Handler]:
        all_handlers: Set[logging.Handler] = set()
        for logger in cls._get_current_loggers():
            for handler in cls._get_logger_handlers(logger):
                all_handlers.add(handler)
        return list(all_handlers)

    @staticmethod
    def _create_log_directory(filename: str) -> None:
        directory = Path(filename).parent
        if not directory.is_dir():
            try:
                directory.mkdir(parents=True, exist_ok=True)
            except OSError as ex:
                print(f"Exception trying to create directory {directory} for log file: {safe_str(ex)}")  # can't log here!
                # Don't re-raise, carry on.

    @classmethod
    def _close_existing_handlers(cls) -> None:
        for logger in cls._get_current_loggers():
            for handler in cls._get_logger_handlers(logger):
                handler.close()

    @classmethod
    def _get_logger_dict(cls) -> Dict[str, Any]:
        if cls.__logger_dict is None:
            logging.getLogger(__name__)  # forces the logging system to initialise, including attaching the manager to the root logger, if not done already
            if hasattr(logging.root, 'manager'):  # This will always be the case unless something has gone terribly wrong with the logging system
                cls.__logger_dict = cast(Dict[str, Any], getattr(logging.root, 'manager').loggerDict)
            else:
                cls.__logger_dict = {}
        return cls.__logger_dict

    @classmethod
    def _print_logger_info(cls, logger: logging.Logger, indent: str = '', handler_found: bool = False) -> None:
        print(f'{indent}*Logging info for "{logger.name}":')
        print(f'{indent}  Logger type: {get_fully_qualified_name(logger)}')
        print(f'{indent}  Logger disabled: {logger.disabled}')
        print(f'{indent}  Logger level: {logger.level}')
        if logger.handlers:
            print(f'{indent}  Logger handlers:')
            for handler in logger.handlers:
                cls._print_handler_info(handler, indent=indent + '    ')
            handler_found = True
        if logger.propagate and logger.parent:
            print(f'{indent}  Logger propagates to:')
            cls._print_logger_hierarchy_item(logger.parent, indent=indent + '  ', handler_found=handler_found)
        if not handler_found and not (logger.propagate and logger.parent):
            if logging.lastResort:
                print(f'{indent}  NO HANDLERS IN HIERARCHY, fall back to handler-of-last-resort:')
                cls._print_handler_info(logging.lastResort, indent=indent + '    ')
            else:
                print(f'{indent}  NO HANDLERS IN HIERARCHY, AND NO HANDLER OF LAST RESORT')

    @classmethod
    def _print_logger_hierarchy_item(cls, item: Union[logging.Logger, logging.PlaceHolder], indent: str, handler_found: bool) -> None:
        if _robust_is_instance(item, logging.Logger):
            cls._print_logger_info(cast(logging.Logger, item), indent, handler_found)
        elif _robust_is_instance(item, logging.PlaceHolder):
            cls._print_placeholder_info(cast(logging.PlaceHolder, item), indent, handler_found)
        else:
            print(f'{indent}UNKNOWN ITEM {get_fully_qualified_name(item)}')

    @classmethod
    def _print_placeholder_info(cls, placeholder: logging.PlaceHolder, indent: str, handler_found: bool) -> None:
        print(f'{indent}*Placeholder with children:')
        children_map = getattr(placeholder, 'loggerMap', {})  # this is actually used like a set: all the values are None
        for child in children_map.keys():
            cls._print_logger_hierarchy_item(child, indent=indent + '  ', handler_found=handler_found)

    @staticmethod
    def _print_handler_info(handler: logging.Handler, indent: str) -> None:
        print(f'{indent}Handler type: {get_fully_qualified_name(handler)}:')
        print(f'{indent}  Handler level: {handler.level}')
        filename = getattr(handler, 'baseFilename', None)
        if filename:
            print(f'{indent}  Handler filename: {filename}')
        else:
            stream = getattr(handler, 'stream', None)
            if stream:
                print(f'{indent}  Handler stream: {"<stdio>" if stream == sys.stdout else "<stderr>" if stream == sys.stderr else stream}')
        queue = getattr(handler, 'queue', None)
        if queue:
            print(f'{indent}  Handler queue: {queue}')
