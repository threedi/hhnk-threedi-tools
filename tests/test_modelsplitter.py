# %%
import shutil
from pathlib import Path

import hhnk_research_tools as hrt
import pytest

from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.modelbuilder import create_base_rasters
from hhnk_threedi_tools.core.schematisation.model_splitter import ModelSchematisations
from tests.config import FOLDER_TEST, PATH_NEW_FOLDER


class TestModelSplitter:
    @pytest.fixture(scope="class")
    def splitter(self):
        FOLDER_NEW = Folders(PATH_NEW_FOLDER, create=True)
        shutil.copytree(FOLDER_TEST.model.schema_base.base, FOLDER_NEW.model.schema_base.base, dirs_exist_ok=True)
        shutil.copy(FOLDER_TEST.model.settings.base, FOLDER_NEW.model.settings.base)
        shutil.copy(FOLDER_TEST.model.settings_default.base, FOLDER_NEW.model.settings_default.base)
        shutil.copy(FOLDER_TEST.model.model_sql.base, FOLDER_NEW.model.model_sql.base)
        spl = ModelSchematisations(folder=FOLDER_NEW)
        return spl

    def test_create_schematisation(self, splitter):
        """tests if the import of information works, if the correct amount is imported"""
        splitter.create_schematisation(name="0d1d_test")
        splitter.create_schematisation(name="1d2d_glg")

        assert splitter.folder.model.schema_1d2d_glg.rasters.initial_wlvl_2d.exists()

    def test_create_local_sqlite_revision(self, splitter):
        local_rev_str = splitter.get_latest_local_revision_str()
        assert local_rev_str.startswith("no previous local") == True

        # Create revision
        splitter.create_local_sqlite_revision("testrevision")

        local_rev_str = splitter.get_latest_local_revision_str()
        assert "testrevision" in local_rev_str

    def test_query(self, splitter):
        splitter.create_schematisation(name="basis_errors")
        database = splitter.folder.model.schema_basis_errors.database

        cross_loc_df = database.read_table("v2_cross_section_location")
        assert 99999 in cross_loc_df["id"].values


# %%
if __name__ == "__main__":
    selftest = TestModelSplitter()
    splitter = selftest.splitter()

    selftest.test_create_local_sqlite_revision(splitter)
