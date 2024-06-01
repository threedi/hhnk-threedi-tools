import logging
import os
import sys

from hhnk_threedi_tools.git_model_repo.utils.get_git_root import get_git_root

log = logging.getLogger(__name__)


def install_git_hook(repo_path, hook_name, script_path, windows=False):
    hook_path = os.path.join(repo_path, ".git", "hooks", hook_name)

    if os.path.isfile(hook_path):
        # check if already added
        with open(hook_path, "r") as hook_file:
            if script_path in hook_file.read():
                print(f"Hook {hook_name} already installed in {hook_path}")
            else:
                with open(hook_path, "a") as hook_file:
                    print(f"add instructions to {hook_path}")
                    hook_file.write(f"\n\n# Added by hhnk_threedi_tools/git_model_repo/install_hooks.py\n")
                    hook_file.write(f'hook_dir=$(realpath "$0")\n"{script_path}" {hook_name} "$hook_dir" "$(pwd)"\n')
                    if windows:
                        pass
                    else:
                        # Maak het script uitvoerbaar
                        os.chmod(hook_path, 0o775)

    else:
        with open(hook_path, "w") as hook_file:
            print(f"create {hook_path}")
            hook_file.write(f"#!/bin/sh\n\n# created by hhnk_threedi_tools/git_model_repo/install_hooks.py\n")
            hook_file.write(f'hook_dir=$(realpath "$0")\n"{script_path}" {hook_name} "$hook_dir" "$(pwd)"\n')
            if windows:
                pass
            else:
                # Maak het script uitvoerbaar
                os.chmod(hook_path, 0o775)


def install_hooks(git_root_dir):
    """Install git hooks in git_root_dir."""
    print("installing hooks for ", git_root_dir)

    # check if git_root_dir is a git repository
    root = get_git_root(git_root_dir)
    if root is None:
        raise ValueError(f"{git_root_dir} is not a git repository")
    if root != git_root_dir:
        # print warning and ask to continue
        log.warning(f"{git_root_dir} is not the root of a git repository. Changed directory to {root}")
        doContinue = input("Do you want to continue? [y/N]")
        if not doContinue or doContinue[0].lower() != "y":
            exit(1)

    hooks = [
        "pre-commit",
        # 'pre-push',
        "commit-msg",
        "post-commit",
        "post-checkout",
        "post-merge",
        "post-rewrite",
        "prepare-commit-msg",
    ]

    # check if windows or linux
    if sys.platform == "win32":
        script_cmd = os.path.join(os.path.dirname(__file__), "bin", "run_hook.bat")
        script_cmd = os.path.abspath(script_cmd)

        for hook in hooks:
            install_git_hook(root, hook, script_cmd, windows=True)

    if sys.platform in ["linux", "darwin", "win32"]:
        script_sh = os.path.join(os.path.dirname(__file__), "bin", "linux", "run_hook.sh")
        script_sh = os.path.abspath(script_sh)

        for hook in hooks:
            install_git_hook(root, hook, script_sh)

    else:
        raise ValueError(f"unknown platform {sys.platform}")

    # create or add to .gitignore all *_backup.gpkg and *_backup.xlsx files
    ignore_file = os.path.join(root, ".gitignore")
    if os.path.isfile(ignore_file):
        with open(ignore_file, "r") as f:
            lines = f.readlines()
    else:
        lines = []

    if not any([l.startswith("*_backup.gpkg") for l in lines]):
        lines.append("*_backup.gpkg\n")
    if not any([l.startswith("*_backup.xlsx") for l in lines]):
        lines.append("*_backup.xlsx\n")

    with open(ignore_file, "w") as f:
        f.writelines(lines)


if __name__ == "__main__":
    install_hooks(sys.argv[1])
