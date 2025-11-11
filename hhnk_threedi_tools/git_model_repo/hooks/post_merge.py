import logging
from pathlib import Path

from hhnk_threedi_tools.git_model_repo.tasks.restore_files_in_directory import restore_files_in_directory
from hhnk_threedi_tools.git_model_repo.utils.timer_log import SubTimer

logger = logging.getLogger(__name__)


def run(repo_root: Path):
    """Called on the post-merge hook.

    This function restores files in the repository after a git merge
    by calling `restore_file_in_directory`

    Parameters
    ----------
    repo_root : Path
        Path to the root of the git repository.

    Returns
    -------
    None
    """
    with SubTimer("restore_file_in_directory"):
        restore_files_in_directory(repo_root)
