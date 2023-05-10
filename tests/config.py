# %%
from pathlib import Path
import shutil
from hhnk_threedi_tools.core.folders import Folders
import copy
import hhnk_research_tools as hrt

TEST_DIRECTORY = Path(__file__).parent.absolute() / "data"

PATH_TEST_MODEL = TEST_DIRECTORY / "model_test"
PATH_NEW_FOLDER = TEST_DIRECTORY / "new_project"


FOLDER_TEST = Folders(PATH_TEST_MODEL)
FOLDER_NEW = Folders(PATH_NEW_FOLDER)


if PATH_NEW_FOLDER.exists():
    shutil.rmtree(PATH_NEW_FOLDER)


