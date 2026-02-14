from pathlib import Path

import pytest

from hhnk_threedi_tools.git_model_repo.hooks import commit_msg


def test_commit_msg_reads_absolute_path(tmp_path):
    repo = tmp_path
    # create an arbitrary commit message file somewhere (simulate git passing absolute path)
    msg_file = tmp_path / "COMMIT_EDITMSG"
    msg_file.write_text("This is a valid commit message\n", encoding="utf-8")

    # should not raise
    commit_msg.run(repo, str(msg_file))


def test_commit_msg_short_message_exits(tmp_path):
    repo = tmp_path
    msg_file = tmp_path / "COMMIT_EDITMSG"
    msg_file.write_text("short\n", encoding="utf-8")

    with pytest.raises(SystemExit):
        commit_msg.run(repo, str(msg_file))
