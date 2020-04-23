import logging


def log_in_secondary_module(message: str) -> None:
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    logger.debug(message)
