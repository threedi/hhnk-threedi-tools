import os
import shutil


def get_local_development_output_dir(clean=False):
    """Get the local development output directory which can be used instead of temp directory
       for development
    """
    path = os.path.join(
        os.path.dirname(__file__),
        'tmp_output'
    )

    if clean:
        if os.path.exists(path):
            shutil.rmtree(path)
        os.makedirs(path, exist_ok=True)

    return path
