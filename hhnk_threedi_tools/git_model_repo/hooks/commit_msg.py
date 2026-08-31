import logging
from os import path
from pathlib import Path

from hhnk_threedi_tools.git_model_repo.utils.setup_logging import setup_logging

logger = logging.getLogger(__name__)
setup_logging(logging.INFO)


def run(repo_root: Path, commit_msg_rel_file_path: str = None):
    """Called on the commit-msg hook.

    The function reads the commit message from the `.git/COMMIT_EDITMSG` file and enforces
    a minimum length of 8 characters.

    Note
    -----
    This hook is not used by 'Github Desktop', so this function is not used.

    Parameters
    ----------
    repo_root : Path
        Path to the root of the git repository.
    commit_msg_rel_file_path : str
        Relative path to the commit message file from the repository root.

    Returns
    -------
    None

    Raises
    ------
    SystemExit
        If the commit message is shorter than 8 characters.
    """

    # if not commit_msg_rel_file_path:
    #     file_path = repo_root / ".git/COMMIT_EDITMSG"
    # else:
    #     file_path = repo_root / commit_msg_rel_file_path

    file_path = repo_root / ".git/COMMIT_EDITMSG"
    commit_msg = path.getsize(file_path)
    logger.info(f"commit message length is {commit_msg} characters")

    if commit_msg < 10:
        logger.info("commit message is too short, minimum of 10 characters required")
        exit(1)
