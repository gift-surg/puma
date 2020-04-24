from puma.buffer import Buffer
from puma.runnable.message import CommandMessage

CommandMessageBuffer = Buffer[CommandMessage]
"""Type for buffers containing command messages to a Runner from its owner."""
