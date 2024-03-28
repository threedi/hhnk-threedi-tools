import os
import sys

from utils.get_git_root import get_git_root


def install_git_hook(repo_path, hook_name, script_path, windows=False):
    hook_path = os.path.join(repo_path, '.git', 'hooks', hook_name)
    with open(hook_path, 'w') as hook_file:
        hook_file.write(f'#!/bin/sh\n\nhook_dir=$(realpath "$0")\n"{script_path}" {hook_name} $hook_dir "$@"\n')
        if windows:
            pass
            # hook_file.write(f'@echo off\n\nset "hook_dir=%~f0"\n{script_path} {hook_name} %hook_dir%\n')
        else:
            # Maak het script uitvoerbaar
            os.chmod(hook_path, 0o775)


def install_hooks(git_root_dir):
    """Install git hooks in git_root_dir.

    """
    print('installing hooks for ', git_root_dir)

    # check if git_root_dir is a git repository
    root = get_git_root(git_root_dir)
    if root is None:
        raise ValueError(f"{git_root_dir} is not a git repository")
    if root != git_root_dir:
        raise ValueError(f"{git_root_dir} is not the root of a git repository. Changed directory to {root}")

    hooks = [
        'pre-commit',
        # 'pre-push',
        'commit-msg',
        'post-commit',
        'post-checkout',
        'post-merge',
        'post-rewrite',
        'prepare-commit-msg',
    ]

    # check if windows or linux
    # if sys.platform == 'win32':
    #     script_cmd = os.path.join(
    #         os.path.dirname(__file__),
    #         'bin',
    #         'run_hook.cmd'
    #     )
    #     script_cmd = os.path.abspath(script_cmd)
    #
    #     for hook in hooks:
    #         install_git_hook(root, hook, script_cmd, windows=True)

    if sys.platform in ['linux', 'darwin', 'win32']:

        script_sh = os.path.join(
            os.path.dirname(__file__),
            'bin',
            'linux',
            'run_hook.sh'
        )
        script_sh = os.path.abspath(script_sh)

        for hook in hooks:
            install_git_hook(root, hook, script_sh)

    else:
        raise ValueError(f"unknown platform {sys.platform}")


if __name__ == '__main__':
    install_hooks(sys.argv[1])
