# %%
if __name__ == "__main__":
    import set_local_paths  # add local git repos.
    
from pathlib import Path
import shutil
from hhnk_threedi_tools.core.folders import Folders
import copy
import hhnk_research_tools as hrt

TEST_DIRECTORY = Path(__file__).parent.absolute() / "data"

PATH_TEST_MODEL = TEST_DIRECTORY / "model_test"
PATH_NEW_FOLDER = TEST_DIRECTORY / "new_project"


# TEMP_DIR = TEST_DIRECTORY/r"temp"
# if TEMP_DIR.exists():
#     shutil.rmtree(TEMP_DIR)
TEMP_DIR = hrt.Folder(TEST_DIRECTORY/r"temp", create=True)
TEMP_DIR.unlink_contents()
TEMP_DIR = TEMP_DIR.pl


FOLDER_TEST = Folders(PATH_TEST_MODEL)
FOLDER_NEW = Folders(PATH_NEW_FOLDER)


if PATH_NEW_FOLDER.exists():
    shutil.rmtree(PATH_NEW_FOLDER)


