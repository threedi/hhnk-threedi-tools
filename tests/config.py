# %%  
from pathlib import Path
import shutil
from hhnk_threedi_tools.core.folders import Folders
import hhnk_research_tools as hrt

TEST_DIRECTORY = Path(__file__).parent.absolute() / "data"

PATH_TEST_MODEL = TEST_DIRECTORY / "model_test"


# TEMP_DIR = TEST_DIRECTORY/r"temp"
# if TEMP_DIR.exists():
#     shutil.rmtree(TEMP_DIR)
TEMP_DIR = hrt.Folder(TEST_DIRECTORY/r"temp", create=True)

TEMP_DIR.unlink_contents()
TEMP_DIR = TEMP_DIR.path
for i in TEMP_DIR.iterdir():
    if i.is_dir:
        for dirname in ["batch_test", "test_project_", "storage_"]:
            if dirname in str(i):
                cont=True

        if cont:
            try:
                shutil.rmtree(i)
            except:
                pass

FOLDER_TEST = Folders(PATH_TEST_MODEL)

PATH_NEW_FOLDER = TEMP_DIR / f"test_project_{hrt.get_uuid()}"
FOLDER_NEW = Folders(PATH_NEW_FOLDER)
