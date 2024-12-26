# %%
import importlib

import hhnk_research_tools as hrt
import pytest

from hhnk_threedi_tools.core import folders_modelbuilder
from hhnk_threedi_tools.core.modelbuilder import create_calculation_rasters
from tests.config import FOLDER_TEST, PATH_NEW_FOLDER, TEMP_DIR

importlib.reload(folders_modelbuilder)
importlib.reload(create_calculation_rasters)
import shutil

from hhnk_threedi_tools.core.folders import Folders


class TestCreateCalculationRasters:
    @pytest.fixture(scope="class")
    def folder_new(self):
        """Copy folder structure and sqlite and then run splitter so we
        get the correct sqlite (with errors) to run tests on.
        """
        FOLDER_NEW = Folders(PATH_NEW_FOLDER, create=True)
        shutil.copytree(FOLDER_TEST.source_data.path, FOLDER_NEW.source_data.path, dirs_exist_ok=True)

        return FOLDER_NEW

    def test_create_polder_tif(folder_new):
        folder = folder_new
        create_calculation_rasters.create_polder_tif(folder, overwrite=True)

        assert folder.model.calculation_rasters.polder.exists()

    def test_create_waterdeel_tif(folder_new):
        folder = folder_new
        create_calculation_rasters.create_waterdeel_tif(folder, overwrite=True)

        assert folder.model.calculation_rasters.waterdeel.exists()


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
    folder_new = TestCreateCalculationRasters().folder_new()
    overwrite = True

    # test_create_calculation_rasters()
