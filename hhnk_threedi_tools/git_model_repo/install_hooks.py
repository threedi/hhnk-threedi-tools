import logging
import sys
from pathlib import Path

if __name__ == "__main__":
    # add the path of the parent directory to the python path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from hhnk_threedi_tools.git_model_repo.utils.get_git_root import get_git_root

logger = logging.getLogger(__name__)


def install_git_hook(repo_path: Path, hook_name: str, script_path: Path, windows: bool = False):
    """Install a git hook script in the repository.

    Parameters
    ----------
    repo_path : Path
        Path to the git repository.
    hook_name : str
        Name of the git hook (e.g., 'pre-commit').
    script_path : Path
        Path to the hook script to be executed.
    windows : bool, optional
        Whether to use Windows-specific settings (default is False).

    Returns
    -------
    None
    """
    hook_path = repo_path / ".git" / "hooks" / hook_name

    if hook_path.is_file():
        with hook_path.open("r") as hook_file:
            if str(script_path) in hook_file.read():
                print(f"Hook {hook_name} already installed in {hook_path}")
            else:
                with hook_path.open("a") as hook_file_append:
                    print(f"add instructions to {hook_path}")
                    hook_file_append.write("\n\n# Added by hhnk_threedi_tools/git_model_repo/install_hooks.py\n")
                    hook_file_append.write(
                        f'hook_dir=$(realpath "$0")\n"{script_path}" {hook_name} "$hook_dir" "$(pwd)"\n'
                    )
                if not windows:
                    hook_path.chmod(0o775)
    else:
        with hook_path.open("w") as hook_file:
            print(f"create {hook_path}")
            hook_file.write("#!/bin/sh\n\n")
            hook_file.write("# created by hhnk_threedi_tools/git_model_repo/install_hooks.py\n")
            hook_file.write(f'hook_dir=$(realpath "$0")\n"{script_path}" {hook_name} "$hook_dir" "$(pwd)"\n')
            if not windows:
                hook_path.chmod(0o775)


def install_hooks(git_root_dir: Path):
    """Install git hooks in the specified git repository root directory.

    Parameters
    ----------
    git_root_dir : Path
        Path to the root of the git repository.

    Returns
    -------
    None
    """
    print("installing hooks for ", git_root_dir)

    root = get_git_root(git_root_dir)
    if root is None:
        raise ValueError(f"{git_root_dir} is not a git repository")
    if root != git_root_dir:
        logger.warning(f"{git_root_dir} is not the root of a git repository. Changed directory to {root}")
        do_continue = input("Do you want to continue? [y/N]")
        if not do_continue or do_continue[0].lower() != "y":
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

    if sys.platform == "win32":
        script_cmd = Path(__file__).parent / "bin" / "run_hook.bat"
        script_cmd = script_cmd.resolve()
        for hook in hooks:
            install_git_hook(root, hook, script_cmd, windows=True)
    elif sys.platform in ["linux", "darwin"]:
        script_sh = Path(__file__).parent / "bin" / "linux" / "run_hook.sh"
        script_sh = script_sh.resolve()
        for hook in hooks:
            install_git_hook(root, hook, script_sh)
    else:
        raise ValueError(f"unknown platform {sys.platform}")

    ignore_file = root / ".gitignore"
    if ignore_file.is_file():
        with ignore_file.open("r") as f:
            lines = f.readlines()
    else:
        lines = []

    ignores = [
        "*_backup.gpkg",
        "*_backup.xlsx",
        "_backup/",
        "*.zip",
        "*_",
        "03_3di_results/",
        "04_test_results/",
        "02_schematisation/*/",
        "!02_schematisation/00_basis*",
        "Notebooks/.*",
    ]

    for ignore in ignores:
        if not any([l.startswith(ignore) for l in lines]):
            lines.append(f"{ignore}\n")

    with ignore_file.open("w") as f:
        f.writelines(lines)


if __name__ == "__main__":
    install_hooks(Path(sys.argv[1]))
