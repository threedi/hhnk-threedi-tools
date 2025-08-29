import logging

logger = logging.getLogger(__name__)


def run(repo_root: str, commit_msg_rel_file_path: str):
    """Called on the prepare-commit-msg hook.

    Note
    -----
    This function is a placeholder for future functionality

    Parameters
    ----------
    repo_root : Path
        Path to the root of the git repository.
    commit_msg_rel_file_path : str
        Relative path to the commit message file from the repository root.

    Returns
    -------
    None
    """
    # todo: implement this function
    print("prepare-commit-msg called, please implement", commit_msg_rel_file_path)
    exit(1)  # Exit immediately to prevent any further processing

    pass
