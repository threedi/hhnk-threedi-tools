import logging

from tasks.restore_files_in_directory import restore_file_in_directory
from utils.setup_logging import setup_logging
from utils.timer_log import SubTimer

log = logging.getLogger(__name__)


def run(repo_root: str):
    with SubTimer("dump_files_in_directory"):
        restore_file_in_directory(repo_root)
