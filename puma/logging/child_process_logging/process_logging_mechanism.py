import logging
from typing import NoReturn, Optional

from puma.context import Exit_1, Exit_2, Exit_3
from puma.logging import Logging, ManagedProcessLogQueue
from puma.logging.child_process_logging.log_listener_process import LogListenerProcess

logger = logging.getLogger(__name__)


class ProcessLoggingMechanism:
    """Implements logging of child processes to a queue, which transports log records back to a central process that logs them.

    An instance of this class is created by ProcessRunner when a child process is run for the first time, and is destroyed when the last child process has ended.
    This class launches and instance of LogListenerProcess, which does the actual logging while child processes are running.
    """

    def __init__(self, logging_queue: ManagedProcessLogQueue) -> None:
        self._logging_queue = logging_queue
        self._log_listener_process: Optional[LogListenerProcess] = None

    def __enter__(self) -> 'ProcessLoggingMechanism':
        self._saved_config = Logging.get_current_logging_config()
        log_listener_config = Logging.get_log_listener_process_logging_config()
        child_logging_config = Logging.get_child_process_logging_config(self._logging_queue)

        # While the logging process is running, it's important that the main process also logs via the logging process, because otherwise
        # it might be trying to write to files that the logging process has open. So at this point the main process has to be configured like a child process.
        Logging.init_child_process_logging(child_logging_config)

        self._log_listener_process = LogListenerProcess(self._logging_queue, log_listener_config)
        logger.debug('Starting log listener...')
        self._log_listener_process.start_blocking()
        logger.debug('Started log listener')

        return self

    def __exit__(self, exc_type: Exit_1, exc_value: Exit_2, traceback: Exit_3) -> None:
        if not self._log_listener_process:
            raise RuntimeError("Internal error: Process Logging Mechanism being stopped in a child process")

        logger.debug('Stopping log listener and restoring main process logging ...')
        self._log_listener_process.stop()
        self._log_listener_process.join(10.0)
        if self._log_listener_process.is_alive():
            print("Failed to stop the log-listener process")  # can't log here!

        # Now that the logging process has ended, the main process must revert to it original logging configuration.
        Logging.restore_current_logging_config(self._saved_config)

        logger.debug('Stopped log listener and restored main process logging')

    def __getstate__(self) -> NoReturn:
        raise RuntimeError("_ProcessLoggingMechanism should only be owned by the main process, not passed to child processes")

    def pause_listener_process(self) -> None:
        # Only used for testing
        assert self._log_listener_process
        self._log_listener_process.pause()

    def resume_listener_process(self) -> None:
        # Only used for testing
        assert self._log_listener_process
        self._log_listener_process.resume()
