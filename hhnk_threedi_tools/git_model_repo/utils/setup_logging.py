import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path

logger = logging.getLogger("")


def setup_logging(level: int = logging.INFO):
    """Setup logging to git_logger.log file in the root of the repo.

    Parameters
    ----------
    level : int, optional
        Logging level (default is logging.INFO).

    Returns
    -------
    None
    """
    logger.setLevel(level)

    log_file_path = Path(__file__).parent.parent / "git_log.log"
    handler = RotatingFileHandler(
        log_file_path,
        maxBytes=1024 * 1024,  # 1 mb
        backupCount=1,
    )
    logger.addHandler(handler)

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)-8s - [%(filename)s-%(lineno)s] - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    logger.debug("Logging setup")
