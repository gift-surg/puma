import logging
from enum import Enum, auto, unique
from typing import Callable, Optional


@unique
class UnexpectedSituationAction(Enum):
    """An enumeration describing the action to take when a certain unexpected action occurs."""
    IGNORE = auto()  # Do nothing (log at debug level only)
    LOG_WARNING = auto()  # Log at warning level
    RAISE_EXCEPTION = auto()  # Raise an exception (by default, RuntimeError)

    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}.{self.name}>"


def handle_unexpected_situation(action: UnexpectedSituationAction,
                                text: str,
                                given_logger: Optional[logging.Logger],
                                exception_factory: Callable[[str], Exception] = RuntimeError) -> None:
    """Implements the appropriate action for a given unexpected situation.

    Arguments:
        action: Option indicating the action to be taken.
        text:   The information to be logged, and included in an exception if raised.
        given_logger: The logger to use when logging, so that the logged text is immediately associated with the source file rather than with this file.
        exception_factory: Method that creates an exception, called if action is RAISE_EXCEPTION. By default, crease a RuntimeError. The parameter is the text string.
    """
    if action == UnexpectedSituationAction.IGNORE:
        if given_logger:
            given_logger.debug(text)
    elif action == UnexpectedSituationAction.LOG_WARNING:
        if given_logger:
            given_logger.warning(text)
    elif action == UnexpectedSituationAction.RAISE_EXCEPTION:
        raise exception_factory(text)
    else:
        raise ValueError("Unrecognised action value in handle_unexpected_situation")
