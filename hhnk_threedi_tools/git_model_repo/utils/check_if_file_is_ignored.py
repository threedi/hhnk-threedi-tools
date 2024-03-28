import os
import typing
from git import Repo

from utils.get_git_root import get_git_root


def is_file_gitignored(file_path, repo: typing.Union[Repo, str, None] = None) -> bool:
    """Check if a file is gitignored.

    Args:
        file_path (str): The path to the file to check.
        repo (typing.Union[Repo, str, None]): The repo to check the file against.
                        Can be Repo instance or repo root path or None (in case repo root path is not known).
                        For performance reasons it is recommended to pass the repo instance if available.
                        Defaults to None.
    Returns:
        bool: True if the file is gitignored, False otherwise.
    """
    if repo is None:
        repo = Repo(get_git_root(file_path))
    elif isinstance(repo, str):
        repo = Repo(repo)

    untracked_files = repo.untracked_files

    # Controleer of het pad relatief is ten opzichte van de repo
    if os.path.isabs(file_path):
        file_path = os.path.relpath(file_path, repo.working_dir)

    return file_path not in untracked_files
