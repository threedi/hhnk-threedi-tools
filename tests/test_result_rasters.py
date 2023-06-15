# %%
"""
Note: the curent tests are only ran to check if the functions work.
They still must be checked qualitatively

"""
if __name__ == "__main__":
    import set_local_paths  # add local git repos.

# First-party imports
from pathlib import Path

# Local imports
from hhnk_threedi_tools.core.folders import Folders
from hhnk_threedi_tools.core.result_rasters.calculate_raster import BaseCalculatorGPKG
# from hhnk_threedi_tools.core.result_rasters.netcdf_to_gridgpkg import BankLevelTest

import pandas as pd
# pd.options.mode.chained_assignment = 'raise' #catch SettingWithCopyWarning

import pytest
from tests.config import FOLDER_TEST
import time




class TestBaseCalculatorGPKG:
    
    @pytest.fixture(scope="class")
    def basecalc(self):
        return BaseCalculatorGPKG(FOLDER_TEST)
      

    def test_basecalc(self):
        calculator_kwargs = {"dem_path":dem_path,
                            "grid_gdf":grid_gdf, 
                            "wlvl_column":"wlvl_max_replaced"}

        #Init calculator
        with BaseCalculatorGPKG(**calculator_kwargs) as self:
            self.run(output_file=threedi_result.pl/"wlvl_corr.tif",  
                        mode="MODE_WLVL",
                        overwrite=OVERWRITE)
            self.run(output_file=threedi_result.pl/"wdepth_corr.tif",  
                        mode="MODE_WDEPTH",
                        overwrite=OVERWRITE)
            print("Done.")

# %%
if __name__ == "__main__":
    import inspect
    selftest = TestResultRasters()
    self = selftest.bl_test
    #Run all testfunctions
    for i in dir(selftest):
        if i.startswith('test_') and hasattr(inspect.getattr_static(selftest,i), '__call__'):
            print(i)
            getattr(selftest, i)()    