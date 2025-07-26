import logging
import os
from logging.handlers import RotatingFileHandler

log = logging.getLogger("")


def setup_logging(level=logging.INFO):
    """Setup logging to git_log.log file in the root of the repo."""
    # log to git_log.log
    log.setLevel(level)

    handler = RotatingFileHandler(
        os.path.join(os.path.dirname(__file__), os.pardir, "git_log.log"),
        maxBytes=1024 * 1024,  # 1 mb
        backupCount=1,
    )
    log.addHandler(handler)

    formatter = logging.Formatter(
        "%(asctime)s %(levelname)-8s - [%(filename)s-%(lineno)s] - %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )

    handler.setFormatter(formatter)

    log.debug("Logging setup")
