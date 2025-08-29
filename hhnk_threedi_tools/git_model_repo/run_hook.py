import logging
import sys
from pathlib import Path

if __name__ == "__main__":
    # add the path of the parent directory to the python path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from hhnk_threedi_tools.git_model_repo.utils.get_git_root import get_git_root
from hhnk_threedi_tools.git_model_repo.utils.setup_logging import setup_logging

logger = logging.getLogger(__name__)
setup_logging(logging.INFO)


def run_hook(hook_name: str, hook_dir: str, *args: str):
    """Run the hook with the specified name.

    Parameters
    ----------
    hook_name : str
        Name of the git hook to run.
    hook_dir : str
        Directory where the hook is executed.
    *args : str
        Additional arguments for the hook.

    Returns
    -------
    None

    Raises
    ------
    ValueError
        If the hook name is unknown or if the directory is not in a git repository.
    """
    logger.info(f"Running hook {hook_name} in directory {hook_dir}. extra args: {args}")

    try:
        root = get_git_root(hook_dir)

        if root is None:
            raise ValueError(f"{hook_dir} is not in a git repository??!!")

        if hook_name == "pre-commit":
            from hhnk_threedi_tools.git_model_repo.hooks.pre_commit import run

            run(root)
        elif hook_name == "commit-msg":
            from hhnk_threedi_tools.git_model_repo.hooks.commit_msg import run

            run(root, args[0])
        elif hook_name == "prepare-commit-msg":
            from hhnk_threedi_tools.git_model_repo.hooks.prepare_commit_msg import run

            run(root, args[0])
        elif hook_name == "post-commit":
            from hhnk_threedi_tools.git_model_repo.hooks.post_commit import run

            run(root)
        elif hook_name == "post-checkout":
            from hhnk_threedi_tools.git_model_repo.hooks.post_checkout import run

            run(root)
        elif hook_name == "post-merge":
            from hhnk_threedi_tools.git_model_repo.hooks.post_merge import run

            run(root)
        elif hook_name == "post-rewrite":
            from hhnk_threedi_tools.git_model_repo.hooks.post_rewrite import run

            run(root)
        else:
            raise ValueError(f"Unknown hook name {hook_name}")
    except Exception as e:
        logger.exception(e)
        raise e


if __name__ == "__main__":
    run_hook(*sys.argv[1:])
