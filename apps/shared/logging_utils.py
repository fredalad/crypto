import logging

LOG_FORMAT = "%(asctime)s | %(levelname)-5s | %(message)s"


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format=LOG_FORMAT,
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
