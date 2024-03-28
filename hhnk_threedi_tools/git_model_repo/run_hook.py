import os
import sys
import logging

if __name__ == '__main__':
    # add the path of the parent directory to the python path
    sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir, os.pardir))

from utils.get_git_root import get_git_root
from utils.setup_logging import setup_logging

log = logging.getLogger(__name__)
setup_logging(logging.INFO)


def run_hook(hook_name, hook_dir, *args):
    """Run the hook with name hook_name.

    """

    log.info(f"Running hook {hook_name} in directory {hook_dir}. extra args: {args}")

    try:
        # check if git_root_dir is a git repository
        root = get_git_root(hook_dir)
        log.info(f"Running hook {hook_name} in root directory {root}. extra args: {args}")

        if root is None:
            raise ValueError(f"{hook_dir} is not in a git repository??!!")

        if hook_name == 'pre-commit':
            from hhnk_threedi_tools.git_model_repo.hooks.pre_commit import run
            run(root)
        elif hook_name == 'commit-msg':
            from hhnk_threedi_tools.git_model_repo.hooks.commit_msg import run
            run(root, args[0])
        elif hook_name == 'prepare-commit-msg':
            from hhnk_threedi_tools.git_model_repo.hooks.prepare_commit_msg import run
            run(root, args[0])
        elif hook_name == 'post-commit':
            from hhnk_threedi_tools.git_model_repo.hooks.post_commit import run
            run(root)
        elif hook_name == 'post-checkout':
            from hhnk_threedi_tools.git_model_repo.hooks.post_checkout import run
            run(root)
        elif hook_name == 'post-merge':
            from hhnk_threedi_tools.git_model_repo.hooks.post_merge import run
            run(root)
        elif hook_name == 'post-rewrite':
            from hhnk_threedi_tools.git_model_repo.hooks.post_rewrite import run
            run(root)
        else:
            raise ValueError(f"Unknown hook name {hook_name}")
    except Exception as e:
        log.exception(e)
        raise e


if __name__ == '__main__':
    run_hook(*sys.argv[1:])
