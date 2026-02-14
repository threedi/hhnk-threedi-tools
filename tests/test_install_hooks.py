from pathlib import Path

from hhnk_threedi_tools.git_model_repo.install_hooks import install_git_hook


def test_install_hook_forwards_args(tmp_path):
    repo = tmp_path
    git_hooks = repo / ".git" / "hooks"
    git_hooks.mkdir(parents=True)

    script_path = Path("/some/script/path/run_hook.sh")

    install_git_hook(repo, "commit-msg", script_path)

    hook_file = git_hooks / "commit-msg"
    assert hook_file.exists()

    content = hook_file.read_text(encoding="utf-8")

    # ensure the generated hook forwards original hook args ($@) and contains the
    # reference to the script path
    assert "$@" in content
    assert str(script_path) in content
