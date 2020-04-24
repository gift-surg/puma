from dataclasses import dataclass
from typing import Any, Dict

from puma.logging.child_process_logging.current_logging_configuration import CurrentLoggingConfiguration


@dataclass(frozen=True)
class ChildProcessConfiguration:
    child_dict: Dict[str, Any]  # Log settings used by child processes, which log to a queue
    parent_history: CurrentLoggingConfiguration  # History used to arrive at the current configuration
