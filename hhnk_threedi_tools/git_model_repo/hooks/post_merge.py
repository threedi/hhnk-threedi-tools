import logging

from hhnk_threedi_tools.git_model_repo.tasks.restore_files_in_directory import restore_file_in_directory
from hhnk_threedi_tools.git_model_repo.utils.timer_log import SubTimer

log = logging.getLogger(__name__)


def run(repo_root: str):
    with SubTimer("dump_files_in_directory"):
        restore_file_in_directory(repo_root)
