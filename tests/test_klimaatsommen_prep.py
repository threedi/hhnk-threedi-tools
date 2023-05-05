# %%
# -*- coding: utf-8 -*-
if __name__ == "__main__":
    import set_local_paths  # add local git repos.

# First-party imports
import pathlib
import pandas as pd
from pathlib import Path

# Local imports
from hhnk_threedi_tools.core.climate_scenarios.klimaatsommen_prep import KlimaatsommenPrep
from hhnk_threedi_tools.core.folders import Folders


TEST_MODEL = pathlib.Path(__file__).parent.absolute() / "data/model_test/"


def test_klimaatsommenprep():

    folder = Folders(TEST_MODEL)

    SCHADESCHATTER_PATH=Path(r"E:\01.basisgegevens\hhnk_schadeschatter") #TODO naar een localsettings oid verplaatsen?

    klimaatsommenrep = KlimaatsommenPrep(folder=folder,
        batch_name="batch_test",
        cfg_file = SCHADESCHATTER_PATH/'01_data/cfg/cfg_lizard.cfg',
        landuse_file = r"\\corp.hhnk.nl\data\Hydrologen_data\Data\01.basisgegevens\rasters\landgebruik\landuse2019_tiles\combined_rasters.vrt",
        SCHADESCHATTER_PATH=SCHADESCHATTER_PATH
    )
    
    klimaatsommenrep.run(overwrite=True)

    for raster_type in ["depth_max", "damage_total"]:
        scenario_metadata = pd.read_csv(klimaatsommenrep.info_file[raster_type], sep=";")
        assertion_metadata = pd.read_csv(klimaatsommenrep.info_file[raster_type].with_stem(f"{raster_type}_info_expected"), sep=";")
        # scenario_metadata = damage_data.drop(['Unnamed: 0'], axis=1)
        # damage_data.set_index(['file name'], inplace = True)

        pd.testing.assert_frame_equal(scenario_metadata, assertion_metadata)


# %%
if __name__ == "__main__":
    test_klimaatsommenprep()
