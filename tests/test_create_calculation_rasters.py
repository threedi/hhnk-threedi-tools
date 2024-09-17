# %%
import importlib

import hhnk_research_tools as hrt

from hhnk_threedi_tools.core import folders_modelbuilder
from hhnk_threedi_tools.core.modelbuilder import create_calculation_rasters
from tests.config import FOLDER_TEST, TEMP_DIR

importlib.reload(folders_modelbuilder)
importlib.reload(create_calculation_rasters)


def test_create_calculation_rasters():
    """Test creation of model rasters"""
    # %%

    dmg_dem = create_calculation_rasters.DamageDem.from_folder(
        folder=FOLDER_TEST,
        panden_raster=hrt.Raster(TEMP_DIR.joinpath(f"panden_{hrt.get_uuid()}.tif")),
        damage_dem=hrt.Raster(TEMP_DIR.joinpath(f"damage_dem_50cm_{hrt.get_uuid()}.tif")),
    )

    dmg_dem.create()
    # assert folder_rasters.dst.dem.exists() is True
    # assert folder_rasters.dst.dem.shape == [6962, 7686]
    # assert dmg_dem.damage_dem.open_rxr().sum() == 1507350.39453125
    assert dmg_dem.damage_dem.open_rxr().sum().values == 1507349.75


# %%
if __name__ == "__main__":
    test_create_calculation_rasters()
