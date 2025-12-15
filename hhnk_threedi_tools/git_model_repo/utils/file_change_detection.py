import hashlib
from pathlib import Path

import git
from git import Repo


class FileChangeDetection(object):
    """Detects changes in a file by comparing its hash.

    Parameters
    ----------
    file_path : Path
        Path to the file to monitor.

    Attributes
    ----------
    file_path : Path
        Path to the file.
    file_hash : str or None
        Hash of the file contents at initialization.

    Methods
    -------
    get_file_hash()
        Returns the current hash of the file.
    has_changed()
        Returns True if the file hash has changed since initialization.
    update_hash()
        Updates the stored hash to the current file hash.
    """

    def __init__(self, file_path: Path):
        """Initialize the FileChangeDetection object.

        Parameters
        ----------
        file_path : Path
            Path to the file to monitor.
        """
        self.file_path = file_path
        self.file_hash = self.get_file_hash()

    def get_file_hash(self):
        """Get the current hash of the file.

        Returns
        -------
        str or None
            MD5 hash of the file contents, or None if the file does not exist.
        """
        if not self.file_path.exists():
            return None
        with self.file_path.open("rb") as f:
            return hashlib.md5(f.read()).hexdigest()

    def has_changed(self):
        """Check if the file hash has changed since initialization.

        Returns
        -------
        bool
            True if the file hash has changed, False otherwise.
        """
        return self.get_file_hash() != self.file_hash

    def update_hash(self):
        """Update the stored hash to the current file hash.

        Returns
        -------
        None
        """
        self.file_hash = self.get_file_hash()


def is_file_git_modified(repo: Repo, rel_file_path: str):
    """Detect if a file is modified, staged, or untracked in a git repository.

    Parameters
    ----------
    repo : Repo
        GitPython Repo object for the repository.
    rel_file_path : str
        Relative path to the file (using forward slashes).

    Returns
    -------
    bool
        True if the file is modified, staged, or untracked; False otherwise.
    """
    relpath_linux_like = rel_file_path.replace("\\", "/")
    try:
        # Check for changes in the working directory (untracked or modified)
        diff_index = repo.index.diff(None)
        for diff in diff_index:
            if diff.a_path == relpath_linux_like or diff.b_path == relpath_linux_like:
                return True

        # Check for staged changes
        staged_diff = repo.index.diff("HEAD")
        for diff in staged_diff:
            if diff.a_path == relpath_linux_like or diff.b_path == relpath_linux_like:
                return True

        # Check for untracked files
        untracked_files = repo.untracked_files
        if relpath_linux_like in untracked_files:
            return True

        return False
    except Exception as e:
        print(f"Error checking file changes: {e}")
        return False
