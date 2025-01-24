# %%
import shutil
from pathlib import Path

import hhnk_research_tools as hrt

from hhnk_threedi_tools.core.folders import Folders

TEST_DIRECTORY = Path(__file__).parent.absolute() / "data"

PATH_TEST_MODEL = TEST_DIRECTORY / "model_test"


TEMP_DIR = hrt.Folder(TEST_DIRECTORY / r"temp", create=True)

TEMP_DIR.unlink_contents()
TEMP_DIR = TEMP_DIR.path
for i in TEMP_DIR.iterdir():
    if i.is_dir:
        cont = False
        if "batch_test" in str(i):
            cont = True
        if "test_project_" in str(i):
            cont = True

        if cont:
            try:
                shutil.rmtree(i)
            except:
                pass

FOLDER_TEST = Folders(PATH_TEST_MODEL)

PATH_NEW_FOLDER = TEMP_DIR / f"test_project_{hrt.current_time(date=True)}"
FOLDER_NEW = Folders(PATH_NEW_FOLDER)
