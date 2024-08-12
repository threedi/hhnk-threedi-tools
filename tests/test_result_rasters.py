# %%
import geopandas as gpd
import hhnk_research_tools as hrt

# pd.options.mode.chained_assignment = 'raise' #catch SettingWithCopyWarning
import pytest

from hhnk_threedi_tools.core.result_rasters.grid_to_raster import GridToRaster
from tests.config import FOLDER_TEST, TEMP_DIR, TEST_DIRECTORY

TEST_RESULT_DIR = TEST_DIRECTORY / r"test_result_rasters"


class TestGridToRaster:
    @pytest.fixture(scope="class")
    def grid_gdf(self):
        return gpd.read_file(TEST_RESULT_DIR / "grid_corr_tiny.gpkg", driver="GPKG")

    @pytest.fixture(scope="class")
    def basecalc(self, grid_gdf):
        calculator_kwargs = {
            "dem_path": TEST_RESULT_DIR / "dem_tiny.tif",
            "grid_gdf": grid_gdf,
            "wlvl_column": "wlvl_max_replaced",
        }
        with GridToRaster(**calculator_kwargs) as basecalc:
            return basecalc

    def test_wlvl(self, basecalc):
        output_file = hrt.Folder(TEMP_DIR).full_path(f"wlvl_corr_{hrt.get_uuid()}.tif")
        basecalc.run(output_file=output_file.path, mode="MODE_WLVL", overwrite=True)

        assert output_file.sum() == 43.918495178222656

    def test_wdepth(self, basecalc):
        output_file = hrt.Folder(TEMP_DIR).full_path(f"wdepth_corr_{hrt.get_uuid()}.tif")
        basecalc.run(output_file=output_file.path, mode="MODE_WDEPTH", overwrite=True)
        assert output_file.sum() == 5.868329048156738


# %%
if __name__ == "__main__":
    import inspect

    selftest = TestGridToRaster()
    basecalc = selftest.basecalc(selftest.grid_gdf())
    # self = selftest.bl_test
    # Run all testfunctions
    for i in dir(selftest):
        if i.startswith("test_") and hasattr(inspect.getattr_static(selftest, i), "__call__"):
            print(i)
            getattr(selftest, i)(basecalc)
# %%
