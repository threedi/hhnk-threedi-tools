# %%
# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import set_local_paths  # add local git repos.

# First-party imports
import pathlib
import pandas as pd
from pathlib import Path
import shutil

# Local imports
from hhnk_threedi_tools.core.climate_scenarios.klimaatsommen_prep import KlimaatsommenPrep
from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.folder_helpers import ClimateResult
import hhnk_research_tools as hrt


from tests.config import FOLDER_TEST, TEMP_DIR, TEST_DIRECTORY

def test_klimaatsommenprep():

    folder = FOLDER_TEST

    # SCHADESCHATTER_PATH=Path(r"E:\01.basisgegevens\hhnk_schadeschatter") #TODO naar een localsettings oid verplaatsen?

    klimaatsommenprep = KlimaatsommenPrep(folder=folder,
        batch_name="batch_test",
        cfg_file = 'cfg_lizard.cfg',
        landuse_file = folder.model.schema_base.rasters.landuse,
        # SCHADESCHATTER_PATH=SCHADESCHATTER_PATH
        verify=False
    )
    
    #Rebase the batch_fd so it will always create all output.
    batch_test = TEMP_DIR/f"batch_test_{hrt.get_uuid()}"
    batch_test = ClimateResult(batch_test)
    shutil.copytree(src=klimaatsommenprep.batch_fd.downloads.piek_glg_T10.netcdf.pl,
                    dst=batch_test.downloads.piek_glg_T10.netcdf.pl)

    klimaatsommenprep.batch_fd = batch_test
    
    
    klimaatsommenprep.run(overwrite=True, testing=True) #TODO =True, maar test duurt lang

    for raster_type in ["depth_max", "damage_total"]:
        scenario_metadata = pd.read_csv(klimaatsommenprep.info_file[raster_type], sep=";")
        assertion_metadata = pd.read_csv(TEST_DIRECTORY/fr"test_klimaatsommen/{raster_type}_info_expected.csv", sep=";")
        # scenario_metadata = damage_data.drop(['Unnamed: 0'], axis=1)
        # damage_data.set_index(['file name'], inplace = True)

        pd.testing.assert_frame_equal(scenario_metadata, assertion_metadata)


# %%
if __name__ == "__main__":
    test_klimaatsommenprep()