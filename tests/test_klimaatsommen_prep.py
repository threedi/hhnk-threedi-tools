# %%
# First-party imports
import shutil

import hhnk_research_tools as hrt
import pandas as pd
import pytest

import hhnk_threedi_tools.core.climate_scenarios.klimaatsommen_main as klimaatsommen_main

# Local imports
from hhnk_threedi_tools.core.climate_scenarios.klimaatsommen_prep import (
    KlimaatsommenPrep,
)
from hhnk_threedi_tools.core.folder_helpers import ClimateResult

if __name__ == "__main__":
    import importlib

    importlib.reload(klimaatsommen_main)

from tests.config import FOLDER_TEST, PATH_NEW_FOLDER, TEMP_DIR, TEST_DIRECTORY


class TestKlimaatsommen:
    # @pytest.fixture(scope="class")
    def folder_new(self):
        """Copy folder structure and sqlite and then run splitter so we
        get the correct sqlite (with errors) to run tests on."""
        FOLDER_NEW = Folders(PATH_NEW_FOLDER, create=True)

        shutil.copytree(src=FOLDER_TEST.source_data.path, dst=FOLDER_NEW.source_data.path, dirs_exist_ok=True)
        shutil.copytree(FOLDER_TEST.model.schema_base.path, FOLDER_NEW.model.schema_base.path, dirs_exist_ok=True)
        # shutil.copy(FOLDER_TEST.model.settings.path, FOLDER_NEW.model.settings.path)
        # shutil.copy(FOLDER_TEST.model.settings_default.path, FOLDER_NEW.model.settings_default.path)
        # shutil.copy(FOLDER_TEST.model.model_sql.path, FOLDER_NEW.model.model_sql.path)
        # self.folder=FOLDER_TEST

        # Rebase the batch_fd so it will always create all output.
        batch_test_orig = FOLDER_TEST.threedi_results.batch["batch_test"]
        batch_test_new = FOLDER_NEW.threedi_results.batch["batch_test"]
        shutil.copytree(src=batch_test_orig.downloads.path, dst=batch_test_new.downloads.path, dirs_exist_ok=True)

        # Maskerkaart requires bui_GHG_T1000
        for bui in ["blok", "piek"]:
            shutil.copytree(
                src=batch_test_orig.downloads.piek_glg_T10.netcdf.path,
                dst=getattr(batch_test_new.downloads, f"{bui}_ghg_T1000").netcdf.path,
                dirs_exist_ok=True,
            )

        # Folder opnieuw init omdat er bepaalde bestanden de eerste keer niet bestonden
        FOLDER_NEW = Folders(PATH_NEW_FOLDER)
        return FOLDER_NEW

    # @pytest.fixture(scope="class")
    def klimaatsommen(self, folder_new):
        klimaatsommen = klimaatsommen_main.KlimaatsommenMain(folder_new, testing=True)

        klimaatsommen.widgets.batch_folder_box.value = klimaatsommen.widgets.batch_folder_box.options[1]
        klimaatsommen.widgets.precipitation_zone_box.value = klimaatsommen.widgets.precipitation_zone_box.options[1]
        return klimaatsommen

    def test_klimaatsommenprep_verify():
        """Raises because not all 18 scenarios downloaded"""
        with pytest.raises(Exception):
            klimaatsommenprep = KlimaatsommenPrep(
                folder=FOLDER_TEST,
                batch_name="batch_test",
                cfg_file="cfg_lizard.cfg",
                landuse_file=FOLDER_TEST.model.schema_base.rasters.landuse,
                verify=True,
            )

    def test_a_klimaatsommenprep(self, folder_new):
        klimaatsommenprep = KlimaatsommenPrep(
            folder=folder_new,
            batch_name="batch_test",
            cfg_file="cfg_lizard.cfg",
            landuse_file=FOLDER_TEST.model.schema_base.rasters.landuse,
            verify=False,
        )

        # Run test
        klimaatsommenprep.run(overwrite=True, testing=True)

        # check results
        for raster_type in ["depth_max", "damage_total"]:
            scenario_metadata = pd.read_csv(klimaatsommenprep.info_file[raster_type].path, sep=";")
            assertion_metadata = pd.read_csv(
                TEST_DIRECTORY / rf"test_klimaatsommen/{raster_type}_info_expected.csv", sep=";"
            )

            pd.testing.assert_frame_equal(scenario_metadata, assertion_metadata)

    def test_b_maskerkaart(self, klimaatsommen):
        klimaatsommen.step1_maskerkaart()

        assert klimaatsommen.batch_fd.output.maskerkaart.exists()
        assert klimaatsommen.batch_fd.output.mask_depth_overlast.exists()


# %%
if __name__ == "__main__":
    self = TestKlimaatsommen()
    folder_new = self.folder_new()
    klimaatsommen = self.klimaatsommen(folder_new)

    # self.test_a_klimaatsommenprep(folder_new)
    self.test_b_maskerkaart(klimaatsommen)
