# %%
import importlib

import hhnk_research_tools as hrt

from hhnk_threedi_tools.core import folders_modelbuilder
from hhnk_threedi_tools.core.modelbuilder.create_calculation_rasters import DamageDem
from tests.config import FOLDER_TEST, TEMP_DIR

importlib.reload(folders_modelbuilder)


def test_create_calculation_rasters():
    """Test creation of model rasters"""
    # %%

    dmg_dem = DamageDem.from_folder(
        folder=FOLDER_TEST,
        panden_raster=hrt.Raster(TEMP_DIR.joinpath(f"panden_{hrt.get_uuid()}.tif")),
        damage_dem=hrt.Raster(TEMP_DIR.joinpath(f"damage_dem50cm_{hrt.get_uuid()}.tif")),
    )

    dmg_dem.create()
    # assert folder_rasters.dst.dem.exists() is True
    # assert folder_rasters.dst.dem.shape == [6962, 7686]
    assert dmg_dem.damage_dem.sum() == 1507350.39453125


# %%
if __name__ == "__main__":
    test_create_calculation_rasters()
