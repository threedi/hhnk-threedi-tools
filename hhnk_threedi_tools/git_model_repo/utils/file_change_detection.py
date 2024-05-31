import hashlib
import os


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
