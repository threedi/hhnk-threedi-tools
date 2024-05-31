import logging

from tasks.restore_files_in_directory import restore_files_in_directory
from utils.timer_log import SubTimer

log = logging.getLogger(__name__)


def run(repo_root: str):
    log.info("Running post-checkout hook")

    with SubTimer("restore_files_in_directory"):
        restore_files_in_directory(repo_root)
