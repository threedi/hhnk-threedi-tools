import hashlib
import os

import git


class FileChangeDetection(object):
    def __init__(self, file_path):
        self.file_path = file_path
        self.file_hash = self.get_file_hash()

    def get_file_hash(self):
        if not os.path.exists(self.file_path):
            return None
        with open(self.file_path, 'rb') as f:
            return hashlib.md5(f.read()).hexdigest()

    def has_changed(self):
        return self.get_file_hash() != self.file_hash

    def update_hash(self):
        self.file_hash = self.get_file_hash()


def is_file_git_modified(repo, rel_file_path):
    """Detecteert of een bestand is gewijzigd in een Git-repository."""

    relpath_linux_like = rel_file_path.replace("\\", "/")
    try:
        # Controleer op wijzigingen in de werkdirectory (untracked of modified)
        diff_index = repo.index.diff(None)  # Verschillen met de HEAD
        for diff in diff_index:
            if diff.a_path == relpath_linux_like or diff.b_path == relpath_linux_like:
                return True

        # Controleer op wijzigingen in de index (gestaged)
        staged_diff = repo.index.diff("HEAD")
        for diff in staged_diff:
            if diff.a_path == relpath_linux_like or diff.b_path == relpath_linux_like:
                return True

        # Controleer op niet-gevolgde bestanden
        untracked_files = repo.untracked_files
        if relpath_linux_like in untracked_files:
          return True

        return False  # het bestand is niet gewijzigd
    except Exception as e:
        print(f"Fout bij controle van bestandswijzigingen: {e}")
        return False
