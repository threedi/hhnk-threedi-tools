import logging
import os
import git

from tasks.dump_files_in_directory import dump_files_in_directory
from utils.timer_log import SubTimer

log = logging.getLogger(__name__)

import ctypes  # An included library with Python install.


def Mbox(title, text, style):
    # on windows, show a message box
    if os.name == 'nt':
        return ctypes.windll.user32.MessageBoxW(0, text, title, style)
    elif os.name == 'posix':
        # if mac
        os.system("""osascript -e 'tell app "Finder" to display dialog "{}" with title "{}"'""".format(text, title))
    else:
        # if linux
        print(title + ": " + text)


def run(repo_root: str):
    log.info("Running pre-commit hook")

    with SubTimer("dump_files_in_directory"):
        changed_files = dump_files_in_directory(repo_root)

    if len(changed_files) > 0:
        # some hacky way to force github desktop to regain focus and refresh the changed file list
        Mbox('Nieuwe omzetting', 'Er zijn extra bestanden omgezet, ga terug naar github desktop', 1)

        stderr = "Volgende bestanden nog aangepast/ toegevoegd:\n%s" % "".join(
            [f"- {os.path.relpath(f, repo_root)}\n" for f in changed_files])
        log.info(stderr)
        # write to stderr to show the user what files are changed
        print(stderr)
        exit(1)
