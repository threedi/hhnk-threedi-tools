# %%
import shutil
from pathlib import Path
import pytest

import hhnk_research_tools as hrt
from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.schematisation.model_splitter import ModelSchematisations
from tests.config import FOLDER_TEST, PATH_NEW_FOLDER


class TestModelbuilderRasters():
    
    @pytest.fixture(scope="class")
    def folder_new(self):
        """Copy folder structure and sqlite and then run splitter so we 
        get the correct sqlite (with errors) to run tests on."""
        FOLDER_NEW = Folders(PATH_NEW_FOLDER, create=True)
        shutil.copytree(FOLDER_TEST.model.schema_base.path, 
                        FOLDER_NEW.model.schema_base.path, 
                        dirs_exist_ok=True)
        shutil.copy(FOLDER_TEST.model.settings.path, FOLDER_NEW.model.settings.path)
        shutil.copy(FOLDER_TEST.model.settings_default.path, FOLDER_NEW.model.settings_default.path)
        shutil.copy(FOLDER_TEST.model.model_sql.path, FOLDER_NEW.model.model_sql.path)
        # self.folder=FOLDER_TEST
        spl = ModelSchematisations(folder=FOLDER_NEW)
        spl.create_schematisation(name='basis_errors')