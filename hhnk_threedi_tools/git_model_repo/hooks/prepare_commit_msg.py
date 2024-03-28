import logging
import os

from utils.setup_logging import setup_logging

log = logging.getLogger(__name__)


def run(repo_root: str, commit_msg_file: str):
    log.info("Running prepare-commit-msg hook")

    message_file_path = os.path.join(repo_root, commit_msg_file)

    commit_msg = open(
        os.path.join(repo_root, commit_msg_file),
        'r'
    ).read()

    print(commit_msg.strip() + ': \n\n en nog iets')

    # with open(message_file_path, 'w') as f:
    #     f.write(commit_msg.strip() + ' en nog iets')
