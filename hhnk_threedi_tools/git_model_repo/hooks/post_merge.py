import logging

from utils.setup_logging import setup_logging

log = logging.getLogger(__name__)


def run(repo_root: str):
    setup_logging()
    log.info("Running post-merge hook")
