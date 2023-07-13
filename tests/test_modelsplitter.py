# %%
import shutil
from pathlib import Path
import pytest

import hhnk_research_tools as hrt
from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.checks.model_splitter import ModelSchematisations
from tests.config import FOLDER_TEST, PATH_NEW_FOLDER



class TestModelSplitter():
    
    @pytest.fixture(scope="class")
    def splitter(self):

        FOLDER_NEW = Folders(PATH_NEW_FOLDER, create=True)
        shutil.copytree(FOLDER_TEST.model.schema_base.base, 
                        FOLDER_NEW.model.schema_base.base, 
                        dirs_exist_ok=True)
        shutil.copy(FOLDER_TEST.model.settings.base, FOLDER_NEW.model.settings.base)
        shutil.copy(FOLDER_TEST.model.settings_default.base, FOLDER_NEW.model.settings_default.base)
        self.folder=FOLDER_TEST
        spl = ModelSchematisations(folder=FOLDER_NEW)
        return spl
    

    def test_create_schematisation(self, splitter):
        """tests if the import of information works, if the correct amount is imported"""

        splitter.create_schematisation(name="0d1d_test")

# %%
if __name__=="__main__":
    selftest = TestModelSplitter()
    splitter = selftest.splitter()

    selftest.test_create_schematisation(splitter)