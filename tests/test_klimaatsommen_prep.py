# First-party imports
import shutil

import hhnk_research_tools as hrt
import pandas as pd
import pytest

# Local imports
from hhnk_threedi_tools.core.climate_scenarios.klimaatsommen_prep import (
    KlimaatsommenPrep,
)
from hhnk_threedi_tools.core.folder_helpers import ClimateResult
from hhnk_threedi_tools.core.folders import Folders
from tests.config import FOLDER_TEST, TEMP_DIR, TEST_DIRECTORY


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


def test_klimaatsommenprep():

    # Copy model_test to temp-dir
    model_test = TEMP_DIR / f"model_test_{hrt.get_uuid()}"
    shutil.copytree(
        src=FOLDER_TEST.path,
        dst=model_test,
    )
    klimaatsommenprep = KlimaatsommenPrep(
        folder=Folders(model_test),
        batch_name="batch_test",
        cfg_file="cfg_lizard.cfg",
        landuse_file=FOLDER_TEST.model.schema_base.rasters.landuse,
        verify=False,
    )

    # Run test
    klimaatsommenprep.run(overwrite=True, testing=True)

    # check if intermediate results have been made
    assert klimaatsommenprep.folder.model.manipulated_rasters.dem.exists()
    assert klimaatsommenprep.folder.model.manipulated_rasters.panden.exists()
    assert klimaatsommenprep.folder.model.manipulated_rasters.damage_dem.exists()


    # check results
    for raster_type in ["depth_max", "damage_total"]:
        scenario_metadata = pd.read_csv(klimaatsommenprep.info_file[raster_type].path, sep=";")
        assertion_metadata = pd.read_csv(
            TEST_DIRECTORY / rf"test_klimaatsommen/{raster_type}_info_expected.csv", sep=";"
        )
        # scenario_metadata = damage_data.drop(['Unnamed: 0'], axis=1)
        # damage_data.set_index(['file name'], inplace = True)

        pd.testing.assert_frame_equal(scenario_metadata, assertion_metadata)
