# %%
import geopandas as gpd
import hhnk_research_tools as hrt

# pd.options.mode.chained_assignment = 'raise' #catch SettingWithCopyWarning
from hhnk_threedi_tools.core.result_rasters.grid_to_raster import GridToWaterDepth, GridToWaterLevel
from tests.config import FOLDER_TEST, TEMP_DIR, TEST_DIRECTORY

TEST_RESULT_DIR = TEST_DIRECTORY / r"test_result_rasters"


def test_grid_to_raster():
    grid_gdf = gpd.read_file(TEST_RESULT_DIR / "grid_corr_tiny.gpkg", engine="pyogrio")
    dem_path = TEST_RESULT_DIR / "dem_tiny.tif"
    wlvl_column = "wlvl_max_replaced"

    # WLVL raster
    wlvl_raster = hrt.Folder(TEMP_DIR).full_path(f"wlvl_corr_{hrt.get_uuid()}.tif")

    gridtowlvl = GridToWaterLevel(dem_path=dem_path, grid_gdf=grid_gdf, wlvl_column=wlvl_column)
    gridtowlvl.run(output_file=wlvl_raster, overwrite=True)

    assert wlvl_raster.sum() == 43.86664581298828

    # WDEPTH raster
    wdepth_raster = hrt.Folder(TEMP_DIR).full_path(f"wdepth_corr_{hrt.get_uuid()}.tif")

    gridtowdepth = GridToWaterDepth(dem_path=dem_path, wlvl_path=wlvl_raster)
    gridtowdepth.run(output_file=wdepth_raster, overwrite=True)

    assert wdepth_raster.sum() == 5.917023658752441


# %%
if __name__ == "__main__":
    test_grid_to_raster()
# %%
