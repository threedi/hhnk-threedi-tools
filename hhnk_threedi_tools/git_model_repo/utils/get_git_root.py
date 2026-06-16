from pathlib import Path

from git import Repo


def get_git_root(path: Path) -> Path:
    """Get the root directory of a git repository.

    Parameters
    ----------
    path : Path
        Path to a file or directory within the git repository.

    Returns
    -------
    Path
        Path to the root of the git repository.
    """
    repo = Repo(path, search_parent_directories=True)
    return Path(repo.git.rev_parse("--show-toplevel"))
