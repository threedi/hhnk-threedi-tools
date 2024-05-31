import logging
import os

from utils.setup_logging import setup_logging

log = logging.getLogger(__name__)


def run(repo_root: str, commit_msg_file: str):
    # this hook is missing link to the commit message file...

    commit_msg = open(
        os.path.join(repo_root, ".git/COMMIT_EDITMSG"),
        'r'
    ).read()

    if len(commit_msg) < 8:
        print("commit message is too short")
        exit(1)
