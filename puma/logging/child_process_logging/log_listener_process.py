import logging
from logging import LogRecord
from logging.handlers import QueueHandler, QueueListener
from multiprocessing import Process

from puma.buffer.implementation.managed_queues import ManagedProcessQueue
from puma.logging import Logging
from puma.logging.child_process_logging.current_logging_configuration import LogListenerProcessConfiguration
from puma.primitives import ProcessEvent


class LogListenerProcess(Process):
    """This process is started by ProcessLoggingMechanism. It receives log records on a queue and logs them as described by the current logging configuration.

    This process will be started by the logging mechanism if any ProcessRunner is executed. It takes over logging, both from child processes and from the main process,
    so that logging is managed and so that logging to files can succeed without multiple processes trying to open the same files.
    """

    class MyHandler(QueueHandler):
        def __init__(self, queue: ManagedProcessQueue[LogRecord], unpause_event: ProcessEvent):
            self._unpause_event = unpause_event
            super().__init__(queue)

        def handle(self, record: LogRecord) -> None:
            self._unpause_event.wait()
            logger = logging.getLogger(record.name)
            logger.handle(record)

    def __init__(self, logging_queue: ManagedProcessQueue[LogRecord], logging_config: LogListenerProcessConfiguration) -> None:
        super().__init__(name="Log Listener")
        self._q = logging_queue
        self._logging_config = logging_config
        self._stop_event = ProcessEvent()
        self._running_event = ProcessEvent()
        self._unpause_event = ProcessEvent()
        self._unpause_event.set()

    def run(self) -> None:
        Logging.init_log_listener_process_logging(self._logging_config)

        q_listener = QueueListener(self._q, LogListenerProcess.MyHandler(self._q, self._unpause_event))
        q_listener.start()
        self._running_event.set()

        # Block until told to stop
        self._stop_event.wait()

        # End
        self.resume()
        q_listener.stop()

    def start_blocking(self) -> None:
        self.start()
        self._running_event.wait()

    def stop(self) -> None:
        self._stop_event.set()

    def pause(self) -> None:
        # For testing only
        self._unpause_event.clear()

    def resume(self) -> None:
        # For testing only
        self._unpause_event.set()
