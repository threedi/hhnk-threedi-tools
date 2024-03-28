from git import Repo


def get_git_root(path: str) -> str:
    """Get the root of a git repo.

    Args:
        path (str): The path to a file or directory in the git repo.

    Returns:
        str: The path to the root of the git repo.
    """

    repo = Repo(path, search_parent_directories=True)
    return repo.git.rev_parse("--show-toplevel")
