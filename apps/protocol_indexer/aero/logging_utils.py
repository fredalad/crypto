import logging

LOG_NAME = "aero_indexer"
log = logging.getLogger(LOG_NAME)


def setup_logging(level: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-5s | %(message)s",
    )
