# %%
if __name__ == "__main__":
    import set_local_paths  # add local git repos.

# First-party imports
from pathlib import Path
import geopandas as gpd


# Local imports
import hhnk_research_tools as hrt
from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.result_rasters.calculate_raster import BaseCalculatorGPKG
# from hhnk_threedi_tools.core.result_rasters.netcdf_to_gridgpkg import BankLevelTest

import pandas as pd
# pd.options.mode.chained_assignment = 'raise' #catch SettingWithCopyWarning

import pytest
from tests.config import FOLDER_TEST, TEST_DIRECTORY, TEMP_DIR
import time

TEST_RESULT_DIR = TEST_DIRECTORY/r"test_result_rasters"


class TestBaseCalculatorGPKG:
    @pytest.fixture(scope="class")
    def grid_gdf(self):
        return gpd.read_file(TEST_RESULT_DIR/"grid_corr_tiny.gpkg", driver="GPKG")

    @pytest.fixture(scope="class")
    def basecalc(self, grid_gdf):
        calculator_kwargs = {"dem_path":TEST_RESULT_DIR/"dem_tiny.tif",
                    "grid_gdf":grid_gdf, 
                    "wlvl_column":"wlvl_max_replaced"}
        with BaseCalculatorGPKG(**calculator_kwargs) as basecalc:
            return basecalc
      

    def test_wlvl(self, basecalc):
        output_file = hrt.Raster(TEMP_DIR/f"wlvl_corr_{hrt.get_uuid()}.tif")
        basecalc.run(output_file=output_file.pl,  
                        mode="MODE_WLVL",
                        overwrite=True)
        
        assert output_file.sum() == 43.918495178222656

    def test_wdepth(self, basecalc):
        output_file = hrt.Raster(TEMP_DIR/f"wdepth_corr_{hrt.get_uuid()}.tif")
        basecalc.run(output_file=output_file.pl,  
                    mode="MODE_WDEPTH",
                    overwrite=True)
        assert output_file.sum() == 5.868329048156738
    

# %%
if __name__ == "__main__":
    import inspect
    selftest = TestBaseCalculatorGPKG()
    basecalc = selftest.basecalc(selftest.grid_gdf())
    # self = selftest.bl_test
    #Run all testfunctions
    for i in dir(selftest):
        if i.startswith('test_') and hasattr(inspect.getattr_static(selftest,i), '__call__'):
            print(i)
            getattr(selftest, i)(basecalc)    
# %%
