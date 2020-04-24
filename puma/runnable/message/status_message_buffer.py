from puma.buffer import Buffer
from puma.runnable.message import StatusMessage

StatusMessageBuffer = Buffer[StatusMessage]
"""Type for buffers containing status messages from a Runner to its owner."""
