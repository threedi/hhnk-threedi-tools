# %%
import shutil
from pathlib import Path
import pytest

import hhnk_research_tools as hrt
from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.schematisation.model_splitter import ModelSchematisations
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
        shutil.copy(FOLDER_TEST.model.model_sql.base, FOLDER_NEW.model.model_sql.base)
        spl = ModelSchematisations(folder=FOLDER_NEW)
        return spl
    

    def test_create_schematisation(self, splitter):
        """tests if the import of information works, if the correct amount is imported"""

        splitter.create_schematisation(name="1d2d_glg")

        assert splitter.folder.model.schema_1d2d_glg.rasters.initial_wlvl_2d.exists()


    def test_query(self, splitter):
        splitter.create_schematisation(name="basis_errors")
        database = splitter.folder.model.schema_basis_errors.database

        cross_loc_df=database.read_table("v2_cross_section_location")
        assert 99999 in cross_loc_df["id"].values




# %%
if __name__=="__main__":
    selftest = TestModelSplitter()
    splitter = selftest.splitter()

    selftest.test_create_schematisation(splitter)

