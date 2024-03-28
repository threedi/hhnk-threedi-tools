import logging
import os

from utils.setup_logging import setup_logging

log = logging.getLogger(__name__)


def run(repo_root: str, commit_msg_file: str):
    log.info("Running commit_msg hook")

    commit_msg = open(
        os.path.join(repo_root, commit_msg_file),
        'r'
    ).read()

    print(commit_msg)
    print("fout in commit message")
    exit(1)
