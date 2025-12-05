import shutil
from pathlib import Path


def get_local_development_output_dir(clean: bool = False) -> Path:
    """Gets the local development output directory for development purposes.

    Parameters
    ----------
    clean : bool, optional
        If True, the directory will be cleaned and recreated.

    Returns
    -------
    Path
        Path to the local development output directory.
    """
    path = Path(__file__).parent / "tmp_output"

    if clean:
        if path.exists():
            shutil.rmtree(path)
        path.mkdir(exist_ok=True)

    return path
