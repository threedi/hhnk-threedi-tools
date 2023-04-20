import shutil
import pathlib

from hhnk_threedi_tools.core.folders import Folders

TEST_DIRECTORY = pathlib.Path(__file__).parent.absolute() / "data"
FOLDER = TEST_DIRECTORY / "new_project"
MODEL_FOLDER = TEST_DIRECTORY / "model_test"
SUB_FOLDERS = ["01_Source_data", "02_schematisation", "03_3di_results", "04_test_results"]

def is_empty_dir(dir_path):
    empty = next((dir_path.iterdir()), False)
    return not empty

def test_create_project():
    """tests if a new project folders are created"""
    if FOLDER.exists():
        shutil.rmtree(FOLDER)
    # create a project without creating sub-dirs
    folder = Folders(FOLDER, create=False)
    assert is_empty_dir(FOLDER)    # check sub-dirs empty
    folder.create_project()

    # check if SUB_FOLDERS exist and contain a readme.txt
    for i in SUB_FOLDERS:
        assert FOLDER.joinpath(i).exists()
        assert FOLDER.joinpath(i, "read_me.txt").exists()
