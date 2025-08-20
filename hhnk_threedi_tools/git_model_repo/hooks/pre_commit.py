import logging
import os
from pathlib import Path
from typing import List

from hhnk_threedi_tools.git_model_repo.tasks.dump_files_in_directory import dump_files_in_directory
from hhnk_threedi_tools.git_model_repo.utils.show_message_mbox import show_message_mbox
from hhnk_threedi_tools.git_model_repo.utils.timer_log import SubTimer

log = logging.getLogger(__name__)


def run(repo_root: Path):
    """Called on the pre-commit hook.

    Dumps files in the repository before a commit. If extra files are converted,
    notifies the user and prints the list of changed files. Exits with error if
    there are additional files to commit, so the user can add these to selected files
    for the commit.

    Parameters
    ----------
    repo_root : Path
        Path to the root of the git repository.

    Returns
    -------
    None
    """
    log.info("Running pre-commit hook")

    with SubTimer("dump_files_in_directory"):
        changed_files: List[Path] = dump_files_in_directory(repo_root)

    if len(changed_files) > 0:
        # some hacky way to force GitHub Desktop to regain focus and refresh the changed file list
        show_message_mbox("Nieuwe omzetting", "Er zijn extra bestanden omgezet, ga terug naar Github Desktop")

        # add logging, so it will be displayed in GitHub Desktop
        stderr = "Volgende bestanden nog aangepast/ toegevoegd:\n%s" % "".join(
            [f"- {f.relative_to(repo_root)}\n" for f in changed_files]
        )
        log.info(stderr)
        # write to stderr to show the user what files are changed
        print(stderr)
        exit(1)
