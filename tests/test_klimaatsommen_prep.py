# %%
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


# TODO disabled until new rioxarray calcs
# def test_klimaatsommenprep():
#     """Test creation of gpkg and rasters"""
#     # %%
#     klimaatsommenprep = KlimaatsommenPrep(
#         folder=FOLDER_TEST,
#         batch_name="batch_test",
#         cfg_file="cfg_lizard.cfg",
#         landuse_file=FOLDER_TEST.model.schema_base.rasters.landuse,
#         verify=False,
#     )

#     # Rebase the batch_fd so it will always create all output.
#     batch_test = TEMP_DIR / f"batch_test_{hrt.get_uuid()}"
#     batch_test = ClimateResult(batch_test, create=True)
#     shutil.copytree(
#         src=klimaatsommenprep.batch_fd.downloads.piek_glg_T10.netcdf.path,
#         dst=batch_test.downloads.piek_glg_T10.netcdf.path,
#     )
#     klimaatsommenprep.batch_fd = batch_test

#     # Run test
#     klimaatsommenprep.run(overwrite=True, testing=True)

#     # check results
#     for raster_type in ["depth_max", "damage_total"]:
#         scenario_metadata = pd.read_csv(klimaatsommenprep.info_file[raster_type].path, sep=";")
#         assertion_metadata = pd.read_csv(
#             TEST_DIRECTORY / rf"test_klimaatsommen/{raster_type}_info_expected.csv", sep=";"
#         )
#         # we ignore the order of the columns (check_like=True)
#         pd.testing.assert_frame_equal(scenario_metadata, assertion_metadata, check_like=True)


# %%
# if __name__ == "__main__":
# test_klimaatsommenprep()
