import logging
from pathlib import Path

from hhnk_threedi_tools.git_model_repo.tasks.restore_files_in_directory import restore_files_in_directory
from hhnk_threedi_tools.git_model_repo.utils.timer_log import SubTimer

log = logging.getLogger(__name__)


def run(repo_root: Path):
    """Called on the post-checkout hook.

    This function restores files in the repository after a git checkout
    by calling `restore_files_in_directory`.

    Parameters
    ----------
    repo_root : Path
        Path to the root of the git repository.

    Returns
    -------
    None
    """
    log.info("Running post-checkout hook")

    with SubTimer("restore_files_in_directory"):
        restore_files_in_directory(repo_root)
