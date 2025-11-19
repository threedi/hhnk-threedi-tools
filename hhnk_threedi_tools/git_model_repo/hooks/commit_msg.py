import logging
from pathlib import Path

logger = logging.getLogger(__name__)


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
    if not commit_msg_rel_file_path:
        file_path = repo_root / ".git/COMMIT_EDITMSG"
    else:
        file_path = repo_root / commit_msg_rel_file_path

    file_path = repo_root / ".git/COMMIT_EDITMSG"
    commit_msg = file_path.read_text(encoding="utf-8")

    if len(commit_msg) < 8:
        print("commit message is too short")
        exit(1)
